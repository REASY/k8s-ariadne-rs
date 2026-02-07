pub fn base_prompt(structured: bool) -> String {
    let prompt = ariadne_tools::full_prompt();
    if structured {
        format!(
            "{prompt}\n\nReturn JSON with a single key `cypher` and no extra fields. Do not include explanations or code fences."
        )
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
            "{base}\n\nReturn JSON with keys: answer (string), follow_ups (array of strings, can be empty), confidence (low|medium|high). Always include all keys."
        )
    } else {
        format!("{base}\n\nReturn a short answer followed by a 'Follow-ups:' section if needed.")
    }
}
