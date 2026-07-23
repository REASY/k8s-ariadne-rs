//! Framework-neutral conversation context policy shared by both desktop frontends.
//!
//! This module owns selection, cutoff, and token-budget rules. It deliberately
//! does not know how either GUI stores state or schedules asynchronous work.

use crate::agent::ConversationTurn;
use crate::gui_shared::{FeedItem, FeedState, estimate_text_tokens, estimate_turn_tokens};

pub(crate) const SHORT_TERM_CONTEXT_LIMIT: usize = 4;
pub(crate) const COMPACT_CONTEXT_LIMIT: usize = 12;

const CONTEXT_RESERVED_TOKENS: usize = 2048;
const CONTEXT_MIN_TOKENS: usize = 512;

pub(crate) fn context_budget_tokens(context_window_tokens: Option<usize>) -> Option<usize> {
    let total = context_window_tokens?;
    let budget = total.saturating_sub(CONTEXT_RESERVED_TOKENS);
    Some(budget.max(CONTEXT_MIN_TOKENS).min(total))
}

pub(crate) fn build_context(
    feed: &[FeedItem],
    context_cutoff_id: u64,
    limit: usize,
) -> Vec<ConversationTurn> {
    eligible_turns(feed, context_cutoff_id)
        .take(limit)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

pub(crate) fn build_context_with_budget(
    feed: &[FeedItem],
    context_cutoff_id: u64,
    context_compact_summary: Option<&str>,
    context_window_tokens: Option<usize>,
) -> Vec<ConversationTurn> {
    let Some(budget) = context_budget_tokens(context_window_tokens) else {
        return build_context(feed, context_cutoff_id, SHORT_TERM_CONTEXT_LIMIT);
    };

    let summary_tokens = context_compact_summary
        .map(estimate_text_tokens)
        .unwrap_or(0);
    let mut remaining = budget.saturating_sub(summary_tokens);
    let mut turns = Vec::new();

    for turn in eligible_turns(feed, context_cutoff_id) {
        let turn_tokens = estimate_turn_tokens(&turn);
        if turn_tokens > remaining && !turns.is_empty() {
            break;
        }
        if turn_tokens <= remaining || turns.is_empty() {
            remaining = remaining.saturating_sub(turn_tokens);
            turns.push(turn);
        }
    }
    turns.reverse();
    turns
}

pub(crate) fn filter_suggestions(input: &str, suggestions: &[String]) -> Vec<String> {
    let token = crate::gui_results::current_token(input);
    if token.is_empty() {
        return Vec::new();
    }
    let token = token.to_lowercase();
    suggestions
        .iter()
        .filter(|suggestion| suggestion.to_lowercase().starts_with(&token))
        .take(6)
        .cloned()
        .collect()
}

fn eligible_turns(
    feed: &[FeedItem],
    context_cutoff_id: u64,
) -> impl Iterator<Item = ConversationTurn> + '_ {
    feed.iter().rev().filter_map(move |item| {
        if item.id < context_cutoff_id || !matches!(item.state, FeedState::Ready) {
            return None;
        }
        Some(ConversationTurn {
            question: item.user_text.clone(),
            cypher: item.cypher.clone()?,
            result_summary: item.context_summary.clone(),
            bindings: item.context_bindings.clone(),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_item(id: u64, question: &str) -> FeedItem {
        let mut item = FeedItem::new(id, question.to_string());
        item.state = FeedState::Ready;
        item.cypher = Some(format!("RETURN {id}"));
        item
    }

    #[test]
    fn context_respects_cutoff_limit_and_chronological_order() {
        let feed = vec![
            ready_item(1, "one"),
            ready_item(2, "two"),
            ready_item(3, "three"),
        ];
        let turns = build_context(&feed, 2, 2);
        assert_eq!(
            turns
                .iter()
                .map(|turn| turn.question.as_str())
                .collect::<Vec<_>>(),
            ["two", "three"]
        );
    }

    #[test]
    fn context_budget_is_clamped_to_the_window() {
        assert_eq!(context_budget_tokens(Some(256)), Some(256));
        assert_eq!(context_budget_tokens(Some(4096)), Some(2048));
        assert_eq!(context_budget_tokens(None), None);
    }
}
