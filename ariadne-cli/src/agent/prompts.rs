pub fn base_prompt(structured: bool) -> String {
    let prompt = ariadne_tools::full_prompt();
    if structured {
        let guidance = "Return JSON with keys `cypher` and `params` (array). \
`params` should be a list of objects with keys `key` (string) and `value` (string). \
Always include `params`; use [] when there are no parameters. \
The `value` must be a JSON-encoded literal (e.g. \"\\\"name\\\"\", \"42\", \"true\", \"[1,2]\", \"{\\\"k\\\":\\\"v\\\"}\"). \
Do not include extra fields, explanations, or code fences.";
        format!("{prompt}\n\n{guidance}")
    } else {
        format!("{prompt}\n\nReturn only Cypher. Do not include explanations or code fences.")
    }
}

pub fn analysis_compaction_prompt() -> String {
    "You summarize short-term investigation context for future SRE answers. \
Return a concise, plain-text summary with key entities, filters, assumptions, and results. \
Keep it under 1200 characters. Do not return Cypher."
        .to_string()
}

pub fn analysis_prompt(structured: bool) -> String {
    let base = "You are a Kubernetes SRE assistant. Use only the provided Cypher query results to answer the question.\
If the results are empty or insufficient, say so and suggest follow-up questions or Cypher queries for clarity.\
Be concise, actionable, and avoid speculation.";
    if structured {
        format!(
            "{base}\n\nReturn JSON with keys: title (string), summary (string), bullets (array of strings), rows (array of objects), follow_ups (array of strings), confidence (low|medium|high). Always include all keys. Use empty arrays when needed."
        )
    } else {
        format!("{base}\n\nReturn a short answer followed by a 'Follow-ups:' section if needed.")
    }
}

pub fn router_prompt() -> String {
    "You are a routing classifier for a Kubernetes graph query assistant.\n\
Decide whether a single Cypher query is enough (one_shot) or whether a multi-turn agent loop is needed (multi_turn).\n\
Choose multi_turn when the question is multi-hop, ambiguous, missing identifiers, or needs exploration.\n\
Choose one_shot for direct lookups or simple single-hop queries.\n\
Return JSON with key: route (one_shot|multi_turn). Do not include extra fields."
        .to_string()
}

pub fn agentic_prompt(structured: bool) -> String {
    let prompt = ariadne_tools::full_prompt();
    let tail = if structured {
        "You are operating in agentic multi-turn mode.\n\
At each step, output JSON with keys: action (\"query\"|\"final\"), cypher (string), and optional params (object).\n\
Use action=\"query\" for focused probe queries to gather missing facts.\n\
Use action=\"final\" only when you can answer the user with a single Cypher query.\n\
Do not include explanations or code fences."
    } else {
        "You are operating in agentic multi-turn mode.\n\
At each step, output:\n\
action: query|final\n\
cypher: <cypher>\n\
Use action=query for probe queries, action=final for the final answer query.\n\
Do not include explanations or code fences."
    };
    format!("{prompt}\n\n{tail}")
}
