use ariadne_cypher::parse_query;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
struct Case {
    id: String,
    scenario: String,
    tags: Vec<String>,
    query: String,
    expect_ok: bool,
}

#[derive(Default)]
struct SkipList {
    tokens: Vec<String>,
}

impl SkipList {
    fn load(path: Option<PathBuf>) -> Self {
        let path = path.filter(|p| p.exists());
        let mut tokens = Vec::new();
        if let Some(path) = path {
            if let Ok(content) = fs::read_to_string(path) {
                for line in content.lines() {
                    let trimmed = line.trim();
                    if trimmed.is_empty() || trimmed.starts_with('#') {
                        continue;
                    }
                    tokens.push(trimmed.to_string());
                }
            }
        }
        SkipList { tokens }
    }

    fn should_skip(&self, case: &Case) -> Option<String> {
        for token in &self.tokens {
            if token.starts_with('@') {
                if case.tags.iter().any(|t| t == token) {
                    return Some(token.clone());
                }
                continue;
            }
            if case.id.contains(token) || case.scenario.contains(token) {
                return Some(token.clone());
            }
        }
        None
    }
}

#[test]
fn tck_parser_compat() {
    let root = match env::var("TCK_PATH") {
        Ok(val) => PathBuf::from(val),
        Err(_) => {
            eprintln!("TCK_PATH not set; skipping openCypher TCK parser check.");
            return;
        }
    };

    let skip_path = env::var("TCK_SKIP").ok().map(PathBuf::from).or_else(|| {
        let default = PathBuf::from("ariadne-cypher/tests/tck_skip.txt");
        if default.exists() {
            Some(default)
        } else {
            None
        }
    });
    let skiplist = SkipList::load(skip_path);

    let mut feature_files = Vec::new();
    collect_feature_files(&root, &mut feature_files);

    if feature_files.is_empty() {
        panic!("No .feature files found under {}", root.display());
    }

    let mut cases = Vec::new();
    for path in feature_files {
        cases.extend(parse_feature_file(&path));
    }

    let mut skipped = Vec::new();
    let mut failures = Vec::new();
    let mut total = 0usize;

    for case in cases {
        total += 1;
        if has_outline_placeholder(&case.query) {
            skipped.push((case.id, "outline placeholder".to_string()));
            continue;
        }
        if let Some(reason) = skiplist.should_skip(&case) {
            skipped.push((case.id, reason));
            continue;
        }

        let result = parse_query(&case.query);
        match (case.expect_ok, result.is_ok()) {
            (true, true) => {}
            (false, false) => {}
            (true, false) => failures.push((
                case.id,
                "expected parse success".to_string(),
                result.err().map(|e| e.to_string()).unwrap_or_default(),
            )),
            (false, true) => {
                failures.push((case.id, "expected parse failure".to_string(), String::new()))
            }
        }
    }

    eprintln!(
        "TCK parser: total={}, skipped={}, failed={}",
        total,
        skipped.len(),
        failures.len()
    );

    if !failures.is_empty() {
        let mut details = String::new();
        for (idx, (id, expectation, err)) in failures.iter().take(20).enumerate() {
            details.push_str(&format!(
                "{:02}. {} ({}) {}\n",
                idx + 1,
                id,
                expectation,
                err
            ));
        }
        panic!("TCK parser mismatches:\n{details}");
    }
}

fn collect_feature_files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_feature_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("feature") {
            out.push(path);
        }
    }
}

fn parse_feature_file(path: &Path) -> Vec<Case> {
    let Ok(content) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut cases = Vec::new();

    let mut pending_tags: Vec<String> = Vec::new();
    let mut scenario: Option<Scenario> = None;
    let mut in_query = false;
    let mut query_buf = String::new();

    for line in content.lines() {
        let trimmed = line.trim();
        if !in_query && trimmed.starts_with('@') {
            pending_tags.extend(trimmed.split_whitespace().map(|s| s.to_string()));
            continue;
        }

        if !in_query && is_scenario_start(trimmed) {
            flush_scenario(path, &mut scenario, &mut cases);
            let name = trimmed.splitn(2, ':').nth(1).unwrap_or("").trim();
            scenario = Some(Scenario {
                name: name.to_string(),
                tags: std::mem::take(&mut pending_tags),
                lines: Vec::new(),
                queries: Vec::new(),
            });
            continue;
        }

        let Some(current) = scenario.as_mut() else {
            continue;
        };

        if trimmed.starts_with("\"\"\"") {
            if in_query {
                current.queries.push(query_buf.trim_end().to_string());
                query_buf.clear();
                in_query = false;
            } else {
                in_query = true;
                query_buf.clear();
            }
            continue;
        }

        if in_query {
            query_buf.push_str(line);
            query_buf.push('\n');
        } else {
            current.lines.push(line.to_string());
        }
    }

    if in_query {
        if let Some(current) = scenario.as_mut() {
            current.queries.push(query_buf.trim_end().to_string());
        }
    }

    flush_scenario(path, &mut scenario, &mut cases);

    cases
}

fn flush_scenario(path: &Path, scenario: &mut Option<Scenario>, cases: &mut Vec<Case>) {
    let Some(current) = scenario.take() else {
        return;
    };
    let expect_ok = !scenario_expects_syntax_error(&current.lines);
    for (idx, query) in current.queries.iter().enumerate() {
        if query.trim().is_empty() {
            continue;
        }
        let id = format!("{}::{}#{}", path.display(), current.name, idx + 1);
        cases.push(Case {
            id,
            scenario: current.name.clone(),
            tags: current.tags.clone(),
            query: query.to_string(),
            expect_ok,
        });
    }
}

fn is_scenario_start(trimmed: &str) -> bool {
    trimmed.starts_with("Scenario:")
        || trimmed.starts_with("Scenario Outline:")
        || trimmed.starts_with("Scenario Outline")
}

fn scenario_expects_syntax_error(lines: &[String]) -> bool {
    let joined = lines.join(" ").to_ascii_lowercase();
    joined.contains("syntaxerror") || joined.contains("syntax error")
}

fn has_outline_placeholder(query: &str) -> bool {
    let bytes = query.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'<' {
            let mut j = i + 1;
            let mut has_ident = false;
            while j < bytes.len() {
                let b = bytes[j];
                if b == b'>' {
                    if has_ident {
                        return true;
                    }
                    break;
                }
                if (b as char).is_ascii_alphanumeric() || b == b'_' {
                    has_ident = true;
                } else {
                    break;
                }
                j += 1;
            }
        }
        i += 1;
    }
    false
}

struct Scenario {
    name: String,
    tags: Vec<String>,
    lines: Vec<String>,
    queries: Vec<String>,
}
