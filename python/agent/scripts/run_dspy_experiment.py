from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import dspy

from k8s_graph_agent.agent import GraphMcpClient
from k8s_graph_agent.config import AgentConfig
from k8s_graph_agent.dspy_experiment import (
    CypherExecutionMetric,
    build_cot_v2_examples,
    build_examples,
    build_stratified_split,
    configure_lm,
    evaluate_program,
    load_live_prompt_sections,
    make_cot_program,
    make_cot_program_v2,
    make_program,
    save_json,
)
from k8s_graph_agent.mcp_client import StreamableHttpMcpClient


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a DSPy prompt-optimization spike for English -> Cypher."
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("eval/questions_gold_full.yaml"),
        help="Gold dataset path (default: eval/questions_gold_full.yaml).",
    )
    parser.add_argument(
        "--models",
        default="openai/gpt-5-mini-2025-08-07,gemini-2.5-flash",
        help="Comma-separated models to optimize.",
    )
    parser.add_argument(
        "--teacher-model",
        help=(
            "Optional stronger model used for instruction proposal and demo "
            "bootstrapping, e.g. openai/gpt-5.4. Defaults to self-teaching "
            "when omitted."
        ),
    )
    parser.add_argument(
        "--train-size",
        type=int,
        default=40,
        help="Number of questions to use for training (default: 40).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20260324,
        help="Random seed for the stratified split (default: 20260324).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("eval/dspy_runs"),
        help="Output root for DSPy artifacts (default: eval/dspy_runs).",
    )
    parser.add_argument(
        "--auto",
        default="light",
        help="DSPy MIPROv2 optimization mode: light, medium, heavy, or none (default: light).",
    )
    parser.add_argument(
        "--num-threads",
        type=int,
        default=4,
        help="DSPy optimizer thread count (default: 4).",
    )
    parser.add_argument(
        "--max-bootstrapped-demos",
        type=int,
        default=2,
        help="Max synthesized demos per prompt (default: 2).",
    )
    parser.add_argument(
        "--max-labeled-demos",
        type=int,
        default=2,
        help="Max labeled demos per prompt (default: 2).",
    )
    parser.add_argument(
        "--num-trials",
        type=int,
        help="Optional MIPROv2 trial cap for cheaper smoke runs.",
    )
    parser.add_argument(
        "--minibatch-size",
        type=int,
        default=20,
        help="MIPROv2 minibatch size (default: 20).",
    )
    parser.add_argument(
        "--num-candidates",
        type=int,
        help="Candidate prompt sets to explore when auto is disabled.",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        help="Optional LM temperature override for both student and teacher.",
    )
    parser.add_argument(
        "--program-type",
        choices=["predict", "cot", "cot-v2"],
        default="predict",
        help="Program type: predict (single-step), cot (CoT with schema as input), or cot-v2 (CoT with schema in instruction — no demo bloat). Default: predict.",
    )
    parser.add_argument(
        "--eval-parallelism",
        type=int,
        default=1,
        help="Number of questions to evaluate in parallel during baseline/compiled eval (default: 1).",
    )
    args = parser.parse_args()
    auto_mode = None if args.auto.lower() == "none" or args.num_trials is not None else args.auto
    num_candidates = args.num_candidates
    if auto_mode is None and num_candidates is None:
        num_candidates = max(4, args.num_trials or 4)

    split = build_stratified_split(args.dataset, train_size=args.train_size, seed=args.seed)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = args.output_dir / timestamp
    run_dir.mkdir(parents=True, exist_ok=True)

    agent_config = AgentConfig.from_env()
    mcp = StreamableHttpMcpClient(
        base_url=agent_config.mcp_url,
        timeout_seconds=agent_config.request_timeout_seconds,
        client_name=agent_config.client_name,
        client_version=agent_config.client_version,
        auth_token=agent_config.mcp_auth_token,
    )
    try:
        graph = GraphMcpClient(mcp=mcp)
        sections = load_live_prompt_sections(mcp, sample_question=split.train[0].question)
        if args.program_type == "cot-v2":
            trainset = build_cot_v2_examples(split.train, sections)
            devset = build_cot_v2_examples(split.dev, sections)
        else:
            trainset = build_examples(split.train, sections)
            devset = build_examples(split.dev, sections)

        if args.program_type == "cot-v2":
            _make_prog = make_cot_program_v2
        elif args.program_type == "cot":
            _make_prog = make_cot_program
        else:
            _make_prog = make_program

        save_json(
            run_dir / "manifest.json",
            {
                "dataset": str(args.dataset),
                "models": [m.strip() for m in args.models.split(",") if m.strip()],
                "teacher_model": args.teacher_model,
                "program_type": args.program_type,
                "train_ids": [q.id for q in split.train],
                "dev_ids": [q.id for q in split.dev],
                "auto": args.auto,
                "num_threads": args.num_threads,
                "max_bootstrapped_demos": args.max_bootstrapped_demos,
                "max_labeled_demos": args.max_labeled_demos,
                "num_trials": args.num_trials,
                "num_candidates": num_candidates,
                "minibatch_size": args.minibatch_size,
                "temperature": args.temperature,
                "tunable_instruction": sections.tunable_instruction,
                "schema_reference_chars": len(sections.schema_reference),
                "node_connectivity_chars": len(sections.node_connectivity),
            },
        )

        for model in [m.strip() for m in args.models.split(",") if m.strip()]:
            print(f"[dspy] model={model}")
            student_lm = configure_lm(model, temperature=args.temperature)
            teacher_lm = (
                configure_lm(args.teacher_model, temperature=args.temperature)
                if args.teacher_model
                else None
            )
            dspy.configure(lm=student_lm)
            baseline = _make_prog(sections)
            baseline_eval = evaluate_program(baseline, devset, graph=graph, parallelism=args.eval_parallelism)
            print(
                f"  baseline matched={baseline_eval.counts.exact_match}/"
                f"{baseline_eval.counts.total}"
            )

            optimizer = dspy.MIPROv2(
                metric=CypherExecutionMetric(graph),
                prompt_model=teacher_lm or student_lm,
                task_model=student_lm,
                teacher_settings={"lm": teacher_lm} if teacher_lm else None,
                auto=auto_mode,
                num_candidates=num_candidates,
                num_threads=args.num_threads,
            )
            compile_kwargs = {
                "trainset": trainset,
                "valset": devset,
                "max_bootstrapped_demos": args.max_bootstrapped_demos,
                "max_labeled_demos": args.max_labeled_demos,
                "seed": args.seed,
                "minibatch_size": args.minibatch_size,
            }
            if args.num_trials is not None:
                compile_kwargs["num_trials"] = args.num_trials
            teacher_program = _make_prog(sections) if teacher_lm else None
            compiled = optimizer.compile(
                baseline,
                teacher=teacher_program,
                **compile_kwargs,
            )
            compiled_eval = evaluate_program(compiled, devset, graph=graph, parallelism=args.eval_parallelism)
            print(
                f"  compiled matched={compiled_eval.counts.exact_match}/"
                f"{compiled_eval.counts.total}"
            )

            model_slug = _sanitize_model(model)
            model_dir = run_dir / model_slug
            model_dir.mkdir(parents=True, exist_ok=True)
            save_json(
                model_dir / "report.json",
                {
                    "model": model,
                    "baseline": baseline_eval.as_dict(),
                    "compiled": compiled_eval.as_dict(),
                },
            )
            _save_program_if_supported(compiled, model_dir / "compiled_program")
    finally:
        mcp.close()


def _sanitize_model(model: str) -> str:
    return (
        model.replace("/", "_")
        .replace(":", "_")
        .replace(" ", "_")
        .replace(".", "_")
    )


def _save_program_if_supported(program: dspy.Module, path: Path) -> None:
    if hasattr(program, "save"):
        program.save(str(path), save_program=True)


if __name__ == "__main__":
    main()
