//! Issue #287 review round 2, finding 2 — how far the per-row budget
//! enumeration actually goes, said as a test rather than as a claim.
//!
//! `MAX_QUERY_RETAINED_BYTES` adds ONE row's per-row output budgets to
//! the leaf figure. The compiler holds the back half of that: every
//! `RowBudgets` FIELD is a term of the sum (both are destructured), and
//! every `RowBudget` VARIANT resolves to some field (`row_budget_ceiling`
//! is an exhaustive match). It does not hold the front half — three
//! routes reach a shipped ledger that is absent from the total:
//!
//! 1. a new variant answered with `0`;
//! 2. a new variant answered with an EXISTING field;
//! 3. a new ledger that reuses a variant, or reports through some other
//!    error type entirely.
//!
//! Route 2 is the one a careless author takes: reaching for the nearest
//! plausible ceiling instead of inventing a field. Both #260's original
//! and #287's first two attempts were chains of exhaustive matches, and
//! neither could see it — round 2's instruction was to stop adding
//! links and say what is true.
//!
//! So this is a TRIPWIRE, not a proof, and it is lexical for the same
//! reason the `AggCaps` censuses beside it are: it counts the variants
//! declared in `pipeline.rs` and the fields declared in `charge.rs` and
//! requires them to correspond one-to-one. That catches routes 1 and 2
//! — both leave a variant without a field of its own — and cannot catch
//! route 3, which never declares a variant at all. `RowBudgets`' doc
//! records what closing route 3 would take.

use syn::visit::Visit;

/// `RowBudget`'s variants, from the source that declares them.
fn row_budget_variants() -> Vec<String> {
    let src = include_str!("../src/logql/pipeline.rs");
    let file = syn::parse_file(src).expect("pipeline.rs parses");
    let mut found = None;
    for item in &file.items {
        if let syn::Item::Enum(e) = item
            && e.ident == "RowBudget"
        {
            assert!(found.is_none(), "RowBudget is declared more than once");
            found = Some(e.variants.iter().map(|v| v.ident.to_string()).collect());
        }
    }
    found.expect("RowBudget is declared in pipeline.rs")
}

/// `RowBudgets`' fields, from the source that declares them.
fn row_budgets_fields() -> Vec<String> {
    let src = include_str!("../src/logql/charge.rs");
    let file = syn::parse_file(src).expect("charge.rs parses");
    let mut found = None;
    for item in &file.items {
        if let syn::Item::Struct(s) = item
            && s.ident == "RowBudgets"
        {
            assert!(found.is_none(), "RowBudgets is declared more than once");
            found = Some(
                s.fields
                    .iter()
                    .map(|f| {
                        f.ident
                            .as_ref()
                            .expect("RowBudgets is a named-field struct")
                            .to_string()
                    })
                    .collect(),
            );
        }
    }
    found.expect("RowBudgets is declared in charge.rs")
}

/// Counts, per `RowBudgets` field name, how many `RowBudget` match arms
/// in `row_budget_ceiling` resolve to it — so an arm answered with a
/// field another arm already uses (route 2), or with a literal (route
/// 1), is visible.
#[derive(Default)]
struct ArmAnswers {
    inside: bool,
    answers: Vec<String>,
}

impl Visit<'_> for ArmAnswers {
    fn visit_item_fn(&mut self, node: &syn::ItemFn) {
        if node.sig.ident == "row_budget_ceiling" {
            self.inside = true;
            syn::visit::visit_item_fn(self, node);
            self.inside = false;
        }
    }
    fn visit_arm(&mut self, node: &syn::Arm) {
        if self.inside {
            self.answers.push(match &*node.body {
                syn::Expr::Path(p) => p
                    .path
                    .segments
                    .last()
                    .map(|s| s.ident.to_string())
                    .unwrap_or_default(),
                // Anything that is not a bare binding — a literal `0`, a
                // sum, a call — is not "this variant's own field".
                _ => "<not-a-field>".to_string(),
            });
        }
        syn::visit::visit_arm(self, node);
    }
}

fn ceiling_arm_answers() -> Vec<String> {
    let src = include_str!("../src/logql/charge.rs");
    let file = syn::parse_file(src).expect("charge.rs parses");
    let mut v = ArmAnswers::default();
    v.visit_file(&file);
    v.answers
}

/// Every declared per-row ledger has a term of its OWN in the published
/// total: one variant, one field, one summand — not a variant sharing
/// another's ceiling and not a variant answered with a constant.
#[test]
fn every_row_budget_variant_has_its_own_term_in_the_published_total() {
    let variants = row_budget_variants();
    let fields = row_budgets_fields();
    let answers = ceiling_arm_answers();

    assert!(!variants.is_empty(), "the census found no variants");
    assert_eq!(
        variants.len(),
        fields.len(),
        "{} RowBudget variants against {} RowBudgets fields — a per-row ledger has been \
         added without a term of its own in MAX_QUERY_RETAINED_BYTES (routes 1 and 2 in \
         RowBudgets' doc)",
        variants.len(),
        fields.len()
    );
    assert_eq!(
        answers.len(),
        variants.len(),
        "row_budget_ceiling has {} arms for {} variants",
        answers.len(),
        variants.len()
    );

    let mut sorted = answers.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        answers.len(),
        "two RowBudget variants resolve to the same RowBudgets field ({answers:?}) — the \
         second ledger is not a term of the total"
    );
    let mut want = fields.clone();
    want.sort();
    assert_eq!(
        sorted, want,
        "row_budget_ceiling's arms must answer with the RowBudgets fields themselves, one \
         each — a literal or a shared field means a ledger without a term"
    );
}

/// The arithmetic half, which the compiler does hold, asserted so a
/// field silently zeroed still fails.
#[test]
fn the_published_row_term_is_the_sum_of_the_declared_budgets() {
    use pulsus_read::logql::{MAX_LEAF_RETAINED_BYTES, MAX_QUERY_RETAINED_BYTES};
    let row_term = MAX_QUERY_RETAINED_BYTES - MAX_LEAF_RETAINED_BYTES;
    assert_eq!(row_term, 134_217_728);
    assert_eq!(
        row_term,
        pulsus_read::logql::template::MAX_TEMPLATE_RENDER_BYTES
            + pulsus_read::logql::MAX_JSON_FLATTEN_KEY_BYTES
    );
}
