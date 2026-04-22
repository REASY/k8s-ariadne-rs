# Eval Summary (20260327_044148)

## Run metadata

- Dataset: eval/questions_gold_expanded_dense_v2.yaml
- Questions: 162
- Mode: retry
- Runs: 1
- Models requested: 19
- Model parallelism: 19
- Question parallelism: 16
- Notes:
  - Claude models were routed through `K8S_GRAPH_EVAL_CLAUDE_PROVIDER=openai`.
  - The original all-model batch `20260327_044148` is valid for OpenAI and Claude models.
  - Gemini models from `20260327_044148` were rerun immediately in `20260327_045757` with explicit gateway envs:
    - `GOOGLE_GEMINI_BASE_URL=https://genai-gateway.agoda.is/gemini/v1beta`
    - `GOOGLE_API_KEY=$OPENAI_API_KEY`
    - `GEMINI_API_KEY=$OPENAI_API_KEY`
  - The original Gemini failure cause in `20260327_044148` was missing Gemini env/config in the shell.
  - The Gemini logs there show: `Missing key inputs argument! To use the Google AI API, provide (api_key) arguments.`
  - `deepseek-r1` failed authentication in the original batch because the native DeepSeek provider path had no `DEEPSEEK_*` env configured.
  - `deepseek-r1` was rerun in `20260327_063401` using `K8S_GRAPH_EVAL_DEEPSEEK_PROVIDER=openai`, `K8S_GRAPH_EVAL_PARALLELISM=3`, and tenacity-based rate-limit backoff.

## Successful model summary

| Model                          | Matched | Correctness % | Exact   | Projected | Query Validity % | Exec Error % | Retry % | Retry Success % | Avg Attempts | Avg Latency (ms) | Avg Tokens | Avg Prompt | Avg Output |
|--------------------------------|---------|---------------|---------|-----------|------------------|--------------|---------|-----------------|--------------|------------------|------------|------------|------------|
| openai/gpt-5.4-2026-03-05      | 159/162 | 98.1%         | 126/162 | 33/162    | 100.0%           | 0.0%         | 0.6%    | 100.0%          | 1.01         | 5981.7           | 17819      | 17697      | 122.55     |
| claude-sonnet-4-6              | 158/162 | 97.5%         | 47/162  | 111/162   | 100.0%           | 0.0%         | 3.7%    | 100.0%          | 1.04         | 8798.7           | 26248      | 26006      | 241.81     |
| gemini-3-pro-preview           | 156/162 | 96.3%         | 54/162  | 102/162   | 100.0%           | 0.0%         | 2.5%    | 100.0%          | 1.02         | 13505.3          | 22838      | 21791      | 120.72     |
| gemini-3.1-pro-preview         | 155/162 | 95.7%         | 57/162  | 98/162    | 100.0%           | 0.0%         | 3.7%    | 83.3%           | 1.04         | 13506.9          | 23136      | 22057      | 125.28     |
| gemini-3-flash-preview         | 154/162 | 95.1%         | 57/162  | 97/162    | 100.0%           | 0.0%         | 3.1%    | 100.0%          | 1.03         | 16854.8          | 24509      | 21925      | 114.88     |
| claude-sonnet-4-5-20250929     | 153/162 | 94.4%         | 41/162  | 112/162   | 99.4%            | 0.6%         | 3.7%    | 83.3%           | 1.04         | 9054.9           | 26226      | 26005      | 221.35     |
| claude-opus-4-5-20251101       | 149/162 | 92.0%         | 43/162  | 106/162   | 98.8%            | 1.2%         | 3.7%    | 50.0%           | 1.04         | 9084.4           | 26205      | 26007      | 198.31     |
| claude-opus-4-6                | 149/162 | 92.0%         | 48/162  | 101/162   | 100.0%           | 0.0%         | 3.1%    | 100.0%          | 1.03         | 9069.4           | 26065      | 25849      | 215.93     |
| gemini-2.5-pro                 | 149/162 | 92.0%         | 43/162  | 106/162   | 99.4%            | 0.6%         | 2.5%    | 75.0%           | 1.02         | 12975.6          | 23047      | 21790      | 116.80     |
| deepseek-r1                    | 149/162 | 92.0%         | 53/162  | 96/162    | 99.4%            | 0.6%         | 8.0%    | 92.3%           | 1.08         | 22993.8          | 24697      | 23459      | 1238.16    |
| openai/gpt-5.2-2025-12-11      | 148/162 | 91.4%         | 97/162  | 51/162    | 98.8%            | 1.2%         | 3.7%    | 66.7%           | 1.04         | 7317.2           | 18423      | 18249      | 173.56     |
| gemini-2.5-flash               | 148/162 | 91.4%         | 34/162  | 114/162   | 99.4%            | 0.6%         | 3.1%    | 80.0%           | 1.04         | 3847.2           | 22416      | 22059      | 82.80      |
| gemini-3.1-flash-lite-preview  | 148/162 | 91.4%         | 51/162  | 97/162    | 100.0%           | 0.0%         | 4.9%    | 100.0%          | 1.05         | 3035.8           | 22443      | 22321      | 121.91     |
| claude-haiku-4-5-20251001      | 148/162 | 91.4%         | 43/162  | 105/162   | 98.8%            | 1.2%         | 4.9%    | 62.5%           | 1.05         | 6861.7           | 26577      | 26328      | 249.12     |
| openai/gpt-5-mini-2025-08-07   | 146/162 | 90.1%         | 79/162  | 67/162    | 100.0%           | 0.0%         | 1.2%    | 100.0%          | 1.01         | 20222.0          | 18829      | 17807      | 1021.31    |
| openai/gpt-5.4-mini-2026-03-17 | 144/162 | 88.9%         | 64/162  | 80/162    | 98.1%            | 1.9%         | 9.3%    | 80.0%           | 1.09         | 5197.3           | 19388      | 19242      | 145.42     |
| openai/gpt-5.4-nano-2026-03-17 | 138/162 | 85.2%         | 76/162  | 62/162    | 96.3%            | 3.7%         | 9.3%    | 26.7%           | 1.09         | 5329.4           | 19387      | 19243      | 144.40     |
| openai/gpt-5-nano-2025-08-07   | 129/162 | 79.6%         | 36/162  | 93/162    | 92.6%            | 7.4%         | 27.8%   | 68.9%           | 1.28         | 30358.7          | 26092      | 22525      | 3567.31    |
| gemini-2.5-flash-lite          | 120/162 | 74.1%         | 26/162  | 94/162    | 79.0%            | 21.0%        | 56.2%   | 61.5%           | 1.56         | 2959.4           | 33499      | 33301      | 197.12     |

# Local LLMs

## Eval Summary (20260408_124756)

## Run metadata

- Dataset: eval/questions_gold_expanded_dense_v2.yaml
- Questions: 162
- Mode: retry
- Runs: 1
- Models: 1
- Model parallelism: 1
- Question parallelism: 1
- Prompt source:
  - `ADK_PROMPT_BUNDLE_FILE=eval/run_evals_prompt_review/compiled_prompt_bundle_old_mcp_analyze_question.txt`
- Runtime:
  - Local OpenAI-compatible endpoint: `OPENAI_BASE_URL=http://192.168.0.187:12345`
  - Model: `gemma-4-31b-it`
  - `ADK_MAX_OUTPUT_TOKENS=512`
  - Snapshot-backed Ariadne MCP against the real Memgraph backend, not the in-memory backend
- Notes:
  - This run was intended as a local-model apples-to-apples check using the historical large prompt bundle.
  - The raw run in `20260408_124756` recorded `5/162` execution errors that were later traced to Ariadne validator/parser false negatives, not Memgraph runtime failures.
  - After restarting MCP against the `snapshot/` folder with the Memgraph parse fallback, the run was rescored into `eval/runs_local_models_gemma4_old_prompt_exact/20260408_124756/rescored_snapshot_memgraph`.
  - The five affected questions were `h04`, `h09`, `h11`, `h13`, and `h18`; after rescoring they became `3 projected` and `2 exact`.
  - That lifted the result from `134/162` matched to `139/162`.
  - Additional comparable local runs under the same prompt/runtime setup were recorded in `20260408_144656` (`gemma-4-e4b-it`, `gemma-4-e2b-it`) and `20260408_180721` (`gemma-4-26b-a4b-it`).

## Model summary

| Model              | Matched | Correctness % | Exact  | Projected | Query Validity % | Validator Error % | Exec Error % | Retry % | Retry Success % | Avg Attempts | Avg Latency (ms) | Avg Tokens | Avg Prompt | Avg Output |
|--------------------|---------|---------------|--------|-----------|------------------|-------------------|--------------|---------|-----------------|--------------|------------------|------------|------------|------------|
| gemma-4-31b-it     | 139/162 | 85.8%         | 49/162 | 90/162    | 99.4%            | 0.0%              | 0.0%         | 5.6%    | 88.9%           | 1.06         | 15219            | 22513      | 22362      | 150.80     |
| gemma-4-26b-a4b-it | 131/162 | 80.9%         | 37/162 | 94/162    | 92.6%            | 0.0%              | 1.2%         | 9.9%    | 31.2%           | 1.10         | 22655            | 23482      | 23304      | 177.70     |
| gemma-4-e4b-it     | 80/162  | 49.4%         | 27/162 | 53/162    | 88.3%            | 0.0%              | 2.5%         | 49.4%   | 76.2%           | 1.49         | 36840            | 32011      | 31707      | 303.81     |
| gemma-4-e2b-it     | 49/162  | 30.2%         | 21/162 | 28/162    | 52.5%            | 0.0%              | 1.9%         | 56.8%   | 16.3%           | 1.57         | 20774            | 33606      | 33286      | 319.75     |
