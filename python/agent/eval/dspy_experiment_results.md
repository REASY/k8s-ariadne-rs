# DSPy Experiment Results V2

Date: 2026-03-24

## Methodology

### Goal

Optimize the `English -> Cypher` translator with DSPy while keeping the
MCP-generated schema/connectivity payload fixed.

### What DSPy Is Allowed To Change

- tunable instruction text
- Cypher-generation rules block
- retained few-shot demos

### What Stays Fixed

- live MCP prompt structure
- schema reference section
- node connectivity section
- execution backend and gold answers

### Evaluation Metric

Primary metric: `matched`

`matched` is taxonomy-aware semantic result match:

- `exact`: exact expected columns and rows
- `projected`: expected answer recoverable from a subset of returned columns

Tracked separately:

- `exact_column_match`
- `projected_match`
- `match_type_counts`

### Optimization Metric

Current DSPy optimization uses `_score_match_evaluation(...)` in
[dspy_experiment.py](/work/python/agent/src/k8s_graph_agent/dspy_experiment.py).

This is richer than the original `1.0 / 0.2 / 0.0` reward:

- `exact` / `projected`: `1.0`
- `ordering_mismatch`: high partial credit
- `missing_rows` / `extra_rows` / `extra_and_missing`: graded partial credit
- `grouped_or_aggregated_shape`, `insufficient_columns`, `empty_result`,
  `wrong_semantics`: low partial credit
- invalid / execution failure: `0.0`

### Split Policy

There are two materially different experiment generations:

1. Early runs:
- small gold set
- ordinary stratified 40/20 split
- no group-aware leakage protection

2. Current trustworthy runs:
- expanded gold set
- taxonomy-aware optimization score
- **group-aware split**

Group-aware split keeps all variants of the same canonical question together
using:

- `group_id`
- `source_question_id`
- `generation_type`

This matters because namespace/host variants would otherwise leak semantics
across train and dev.

### Datasets

- [questions_gold_full.yaml](/work/python/agent/eval/questions_gold_full.yaml)
  - `60` total questions
- [questions_gold_expanded_dense.yaml](/work/python/agent/eval/questions_gold_expanded_dense.yaml)
  - `162` total questions
  - `102` generated variants
  - mostly namespace variants, plus a few host variants
- [questions_gold_expanded.yaml](/work/python/agent/eval/questions_gold_expanded.yaml)
  - `180` total questions
  - reaches target size by allowing more empty-result variants

### Recommendation Boundary

Use the expanded dense, group-aware runs as the main basis for decisions.
Earlier 20-question dev runs are still useful for historical context, but they
should not be treated as decisive.

## Executive Summary

- The old exact-only matcher undercounted semantically correct answers.
- The richer taxonomy-aware matcher materially changed reported performance.
- On the original small split, `gemini-2.5-flash` self-teach already looked best.
- On the expanded dense **group-aware** split, that conclusion became stronger:
  `gemini-2.5-flash` self-teach improved from `34/54` to `44/54`.
- `gemini-3-flash-preview` improved only with stronger teachers, topping out at
  `42/54`, still below `gemini-2.5-flash` self-teach.
- `gemini-3.1-flash-lite-preview` was flat across self-teach and all teacher runs.
- On the OpenAI side, `gpt-5.4-mini` improved materially (`35/54 -> 40/54`),
  `gpt-5-mini` stayed flat at a stronger baseline (`41/54`), and the nano-tier
  models stayed weak or nearly flat.
- `claude-opus-4-6` as teacher was flat on the expanded grouped run.
- Teacher-model gains seen on the early 20-question split did not survive as the
  main story once leakage was removed and the dev set grew.

## Result Tiers

### Tier 1: Current Best Evidence

These are the results to trust most.

Dataset:
- [questions_gold_expanded_dense.yaml](/work/python/agent/eval/questions_gold_expanded_dense.yaml)

Split:
- `108` train / `54` dev
- group-aware

| Student model      | Teacher setup     | Baseline matched | Compiled matched | Exact-column | Projected | Delta |
|--------------------|-------------------|------------------|------------------|--------------|-----------|-------|
| `gemini-2.5-flash` | Self-teach        | `34/54`          | `44/54`          | `9/54`       | `35/54`   | `+10` |
| `gemini-2.5-flash` | `claude-opus-4-6` | `35/54`          | `35/54`          | `5/54`       | `30/54`   | `+0`  |

Artifacts:

- [self-teach report](/work/python/agent/eval/dspy_expanded_runs_grouped/self/20260324_122939/gemini-2_5-flash/report.json)
- [claude teacher report](/work/python/agent/eval/dspy_expanded_runs_grouped/claude46/20260324_122939/gemini-2_5-flash/report.json)

Interpretation:

- Self-teaching is the current best DSPy configuration for `gemini-2.5-flash`.
- The bigger gain is mostly in semantic/projected correctness, not strict
  exact-column agreement.
- The larger dev set and grouped split make this substantially more credible
  than the early 20-question runs.

## Tier 1B: Additional Expanded Dense Grouped Runs

Same methodology as Tier 1:

- dataset: [questions_gold_expanded_dense.yaml](/work/python/agent/eval/questions_gold_expanded_dense.yaml)
- split: `108` train / `54` dev
- group-aware
- taxonomy-aware optimization score

| Student model                   | Teacher setup            | Baseline matched | Compiled matched | Delta |
|---------------------------------|--------------------------|------------------|------------------|-------|
| `gemini-3-flash-preview`        | Self-teach               | `38/54`          | `38/54`          | `+0`  |
| `gemini-3-flash-preview`        | `gpt-5.4`                | `39/54`          | `42/54`          | `+3`  |
| `gemini-3-flash-preview`        | `gemini-3.1-pro-preview` | `38/54`          | `38/54`          | `+0`  |
| `gemini-3-flash-preview`        | `claude-opus-4-6`        | `40/54`          | `42/54`          | `+2`  |
| `gemini-3.1-flash-lite-preview` | Self-teach               | `32/54`          | `32/54`          | `+0`  |
| `gemini-3.1-flash-lite-preview` | `gpt-5.4`                | `32/54`          | `32/54`          | `+0`  |
| `gemini-3.1-flash-lite-preview` | `gemini-3.1-pro-preview` | `33/54`          | `33/54`          | `+0`  |
| `gemini-3.1-flash-lite-preview` | `claude-opus-4-6`        | `31/54`          | `31/54`          | `+0`  |
| `openai/gpt-5-mini`             | Self-teach               | `41/54`          | `41/54`          | `+0`  |
| `openai/gpt-5-nano`             | Self-teach               | `33/54`          | `34/54`          | `+1`  |
| `openai/gpt-5.4-mini`           | Self-teach               | `35/54`          | `40/54`          | `+5`  |
| `openai/gpt-5.4-nano`           | Self-teach               | `35/54`          | `35/54`          | `+0`  |

Artifacts:

- [gemini-3-flash-preview / self](/work/python/agent/eval/dspy_expanded_runs_grouped/g3flash_self/20260324_124744/gemini-3-flash-preview/report.json)
- [gemini-3-flash-preview / gpt-5.4](/work/python/agent/eval/dspy_expanded_runs_grouped/g3flash_teacher_gpt54/20260324_124744/gemini-3-flash-preview/report.json)
- [gemini-3-flash-preview / gemini-3.1-pro-preview](/work/python/agent/eval/dspy_expanded_runs_grouped/g3flash_teacher_g31pro/20260324_124744/gemini-3-flash-preview/report.json)
- [gemini-3-flash-preview / claude-opus-4-6](/work/python/agent/eval/dspy_expanded_runs_grouped/g3flash_teacher_claude46/20260324_124744/gemini-3-flash-preview/report.json)
- [gemini-3.1-flash-lite-preview / self](/work/python/agent/eval/dspy_expanded_runs_grouped/g31flashlite_self/20260324_124744/gemini-3_1-flash-lite-preview/report.json)
- [gemini-3.1-flash-lite-preview / gpt-5.4](/work/python/agent/eval/dspy_expanded_runs_grouped/g31flashlite_teacher_gpt54/20260324_124744/gemini-3_1-flash-lite-preview/report.json)
- [gemini-3.1-flash-lite-preview / gemini-3.1-pro-preview](/work/python/agent/eval/dspy_expanded_runs_grouped/g31flashlite_teacher_g31pro/20260324_124744/gemini-3_1-flash-lite-preview/report.json)
- [gemini-3.1-flash-lite-preview / claude-opus-4-6](/work/python/agent/eval/dspy_expanded_runs_grouped/g31flashlite_teacher_claude46/20260324_124744/gemini-3_1-flash-lite-preview/report.json)
- [openai/gpt-5-mini / self](/work/python/agent/eval/dspy_expanded_runs_grouped/gpt5mini_self/20260324_124744/openai_gpt-5-mini/report.json)
- [openai/gpt-5-nano / self](/work/python/agent/eval/dspy_expanded_runs_grouped/gpt5nano_self/20260324_124744/openai_gpt-5-nano/report.json)
- [openai/gpt-5.4-mini / self](/work/python/agent/eval/dspy_expanded_runs_grouped/gpt54mini_self/20260324_124744/openai_gpt-5_4-mini/report.json)
- [openai/gpt-5.4-nano / self](/work/python/agent/eval/dspy_expanded_runs_grouped/gpt54nano_self/20260324_124744/openai_gpt-5_4-nano/report.json)

Interpretation:

- `gemini-3-flash-preview` responds to strong teachers, but still does not beat
  `gemini-2.5-flash` self-teach.
- `gemini-3.1-flash-lite-preview` appears saturated under this DSPy setup.
- `gpt-5-mini` starts from the strongest OpenAI baseline here, but DSPy did not
  improve it.
- `gpt-5.4-mini` is the only OpenAI student in this batch with a clear DSPy gain.
- Nano-tier OpenAI models remain poor optimization targets in this setup.

## Tier 1C: Expanded Dense V2 Reruns For Strong Baselines

These reruns use the updated gold dataset:

- [questions_gold_expanded_dense_v2.yaml](/work/python/agent/eval/questions_gold_expanded_dense_v2.yaml)

Same split policy:

- `108` train / `54` dev
- group-aware
- self-teach
- taxonomy-aware optimization score

These were run to test whether DSPy still helps already strong baseline models
once the gold set is refined.

| Student model                       | Baseline matched | Compiled matched | Exact-column baseline | Exact-column compiled | Projected baseline | Projected compiled | Delta |
|-------------------------------------|------------------|------------------|-----------------------|-----------------------|--------------------|--------------------|-------|
| `gemini-3.1-pro-preview`            | `46/54`          | `46/54`          | `20/54`               | `20/54`               | `26/54`            | `26/54`            | `+0`  |
| `gemini-3-pro-preview`              | `48/54`          | `49/54`          | `25/54`               | `27/54`               | `23/54`            | `22/54`            | `+1`  |
| `openai/gpt-5.4-2026-03-05`         | `48/54`          | `50/54`          | `46/54`               | `41/54`               | `2/54`             | `9/54`             | `+2`  |
| `openai/claude-sonnet-4-6`          | `50/54`          | `50/54`          | `12/54`               | `12/54`               | `38/54`            | `38/54`            | `+0`  |
| `openai/claude-sonnet-4-5-20250929` | `46/54`          | `49/54`          | `8/54`                | `8/54`                | `38/54`            | `41/54`            | `+3`  |

Artifacts:

- [gemini-3.1-pro-preview / self](/work/python/agent/eval/dspy_runs_v2_g31pro/20260325_021754/gemini-3_1-pro-preview/report.json)
- [gemini-3-pro-preview / self](/work/python/agent/eval/dspy_runs_v2_g3pro/20260325_021754/gemini-3-pro-preview/report.json)
- [gpt-5.4 / self](/work/python/agent/eval/dspy_runs_v2_gpt54/20260324_161628/openai_gpt-5_4-2026-03-05/report.json)
- [claude-sonnet-4-6 / self](/work/python/agent/eval/dspy_runs_v2_claude_sonnet_46/20260324_161628/openai_claude-sonnet-4-6/report.json)
- [claude-sonnet-4-5 / self](/work/python/agent/eval/dspy_runs_v2_claude_sonnet_45/20260324_161628/openai_claude-sonnet-4-5-20250929/report.json)

Interpretation:

- `gemini-3.1-pro-preview` appears saturated under this DSPy setup.
- `gemini-3-pro-preview` improves slightly, but only by one question on the dev
  split.
- `gpt-5.4` still has a small DSPy headroom on the updated split, but the gain
  is modest because the baseline is already near the ceiling.
- `claude-sonnet-4-6` appears saturated under this DSPy setup.
- `claude-sonnet-4-5` benefits slightly, mainly by converting a few misses into
  projected semantic matches rather than increasing strict exact-column matches.
- `gemini-2.5-pro` was started on the same setup but aborted for runtime
  reasons before producing a report.

### Tier 2: Taxonomy-Aware Reruns On The 60-Question Gold Set

Dataset:
- [questions_gold_full.yaml](/work/python/agent/eval/questions_gold_full.yaml)

Split:
- `40` train / `20` dev
- taxonomy-aware optimization
- no group-aware variant handling needed, because this set predates the
  expansion variants

| Student model                   | Teacher setup            | Baseline matched | Compiled matched | Exact-column | Projected | Delta |
|---------------------------------|--------------------------|------------------|------------------|--------------|-----------|-------|
| `gemini-2.5-flash`              | Self-teach               | `15/20`          | `18/20`          | `10/20`      | `8/20`    | `+3`  |
| `gemini-2.5-flash`              | `gpt-5.4`                | `15/20`          | `16/20`          | `10/20`      | `6/20`    | `+1`  |
| `gemini-2.5-flash`              | `claude-opus-4-6`        | `15/20`          | `17/20`          | `9/20`       | `8/20`    | `+2`  |
| `gemini-3.1-flash-lite-preview` | `gpt-5.4`                | `14/20`          | `14/20`          | `10/20`      | `4/20`    | `+0`  |
| `gemini-3.1-flash-lite-preview` | `gemini-3.1-pro-preview` | `14/20`          | `14/20`          | `10/20`      | `4/20`    | `+0`  |

Artifacts:

- [self](/work/python/agent/eval/dspy_reruns_scorev2/flash_self/20260324_114400/20260324_114436/gemini-2_5-flash/report.json)
- [gpt-5.4 teacher](/work/python/agent/eval/dspy_reruns_scorev2/flash_teacher_gpt54/20260324_114400/20260324_114436/gemini-2_5-flash/report.json)
- [claude teacher](/work/python/agent/eval/dspy_reruns_scorev2/flash_teacher_claude46/20260324_114400/20260324_114436/gemini-2_5-flash/report.json)
- [g31 flash-lite / gpt-5.4](/work/python/agent/eval/dspy_reruns_scorev2/g31fl_teacher_gpt54/20260324_114400/20260324_114436/gemini-3_1-flash-lite-preview/report.json)
- [g31 flash-lite / gemini-3.1-pro-preview](/work/python/agent/eval/dspy_reruns_scorev2/g31fl_teacher_g31pro/20260324_114400/20260324_114436/gemini-3_1-flash-lite-preview/report.json)

Interpretation:

- These runs already suggested self-teach was strongest for `gemini-2.5-flash`.
- But the dev set was still only `20`, so each question was worth `5pp`.

### Tier 3: Historical Early Runs

These runs were still useful for exploration, but they should be treated as
historical context rather than the final basis for decisions.

Main themes:

- `gpt-5-mini` stayed flat under DSPy in this prompt shape.
- Gemini-family students were more responsive to DSPy than OpenAI mini/nano
  students.
- Teacher effects were inconsistent and depended heavily on split size and
  metric quality.

Historical artifact roots:

- [early self-teaching runs](/work/python/agent/eval/dspy_runs)
- [teacher runs for Gemini 2.5 students](/work/python/agent/eval/dspy_runs_teacher_gpt54)
- [teacher runs for Gemini 3 students](/work/python/agent/eval/dspy_runs_teacher_gpt54_newstudents)
- [OpenAI student smoke runs](/work/python/agent/eval/dspy_runs_teacher_gpt54_openai_students_smoke)

## Best Current Conclusion

If we had to choose one DSPy artifact today, it would be:

- `gemini-2.5-flash`
- self-teaching
- trained/evaluated with the richer taxonomy-aware score
- validated on the expanded dense dataset with a group-aware split

Current winning artifact:

- [compiled self-teach run](/work/python/agent/eval/dspy_expanded_runs_grouped/self/20260324_122939/gemini-2_5-flash/compiled_program)

Runner-up artifacts worth keeping:

- [gemini-3-flash-preview with gpt-5.4 teacher](/work/python/agent/eval/dspy_expanded_runs_grouped/g3flash_teacher_gpt54/20260324_124744/gemini-3-flash-preview/compiled_program)
- [openai/gpt-5.4-mini self-teach](/work/python/agent/eval/dspy_expanded_runs_grouped/gpt54mini_self/20260324_124744/openai_gpt-5_4-mini/compiled_program)

## What We Still Do Not Know

- whether the `44/54` result is stable across multiple seeds
- whether a better teacher can beat self-teach once the split is group-aware
- whether specialized prompt families outperform one universal prompt
- whether the `180` question set is worth using despite its higher empty-result ratio

## Recommended Next Steps

1. Run the expanded dense grouped experiment across multiple seeds.
2. Extract the winning compiled prompt and demos from the self-teach artifact.
3. Test the winning artifact as a selectable runtime translator mode.
4. Add a second expansion wave with genuinely new pattern families, not only
   namespace/host variants.
