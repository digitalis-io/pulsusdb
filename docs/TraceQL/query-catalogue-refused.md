# The TraceQL query catalogue: the 50 refused queries

The response is `docs/api.md` §4's envelope in every case — **`400`**,
`text/plain`, the message and nothing else — and this design changes neither the
parser, the validator nor the planner, so it changes no refusal.

## 47 the parser or the validator refuses

One row per query in `crates/pulsus-traceql/tests/corpus/reject/`,
`unsupported/` and `validate_reject/`. These never reach the storage. The byte
offset is inside the message.

The reason column comes from three places, one per group. For `reject/` and
`unsupported/` it is the failure the corpus's own `.golden` file pins, quoted
from it. For `validate_reject/` the golden holds the parsed query — those four
parse and the semantic pass refuses them — so the reason is the message
`pulsus_traceql::validate` returns, captured into
`docs/TraceQL/measure/validate_messages.tsv` by running the validator over the
four queries; `docs/TraceQL/measure/README.md` gives the test that regenerates
that file. A validator rejection carries no byte offset, which is why the last
column is `-` for those four. One query in the corpus spans two lines
(`reject/string_raw_newline`); its line break is written `\n` in the table so
the row stays one row.

| # | group | name | TraceQL | status | the reason the golden pins | at byte |
|---:|---|---|---|---|---|---:|
| 1 | `reject` | `aggregate_empty` | `{} \| avg() > 1` | `400` | expected an aggregatable field (duration or an attribute), found ')' | 9 |
| 2 | `reject` | `attr_bare_keyword_value` | `{ .a = maybe }` | `400` | expected an intrinsic (name, duration, status, kind) or a scoped attribute (span., resource., or the unscoped . form), found identifier \"maybe\" | 7 |
| 3 | `reject` | `attr_dot_space_after` | `{ . hi }` | `400` | expected an attribute name immediately after '.' (an attribute path is a single unbroken token), found whitespace after '.' | 2 |
| 4 | `reject` | `attr_dot_space_after_scope` | `{ span. hi = 1 }` | `400` | expected an attribute name immediately after '.' (an attribute path is a single unbroken token), found whitespace after '.' | 6 |
| 5 | `reject` | `attr_dot_space_before_scope` | `{ span .hi = 1 }` | `400` | expected '.' immediately after the preceding attribute path segment (an attribute path is a single unbroken token), found whitespace before '.' | 7 |
| 6 | `reject` | `bare_parent_word` | `{ parent = "x" }` | `400` | expected an intrinsic (name, duration, status, kind) or a scoped attribute (span., resource., or the unscoped . form), found identifier \"parent\" | 2 |
| 7 | `reject` | `bare_unscoped_word` | `{ foo = "x" }` | `400` | expected an intrinsic (name, duration, status, kind) or a scoped attribute (span., resource., or the unscoped . form), found identifier \"foo\" | 2 |
| 8 | `reject` | `by_multi_key` | `{ .a = 1 } \| by(.b, .c)` | `400` | expected ')', found ',' | 18 |
| 9 | `reject` | `count_with_argument` | `{} \| count(duration) > 1` | `400` | expected ')' (count() takes no argument), found identifier \"duration\" | 11 |
| 10 | `reject` | `deep_nesting` | `{ ((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((.a = 1)))))))))))) … (150 bytes, in full in the corpus file)` | `400` | nested past the parser's depth limit | 66 |
| 11 | `reject` | `duration_compound` | `{ duration > 1h30m }` | `400` | expected '}', found duration \"30m\" | 15 |
| 12 | `reject` | `duration_fractional_ms` | `{ duration = 0.0000001ms }` | `400` | the duration 0.0000001ms is not a whole number of nanoseconds | 13 |
| 13 | `reject` | `duration_fractional_ns` | `{ duration = 0.1ns }` | `400` | the duration 0.1ns is not a whole number of nanoseconds | 13 |
| 14 | `reject` | `duration_overflow` | `{ duration > 99999999999999999999s }` | `400` | the duration 99999999999999999999s is not valid: duration overflows u64 nanoseconds | 13 |
| 15 | `reject` | `duration_signed_plus` | `{ duration > +2s }` | `400` | expected a field, a literal, or '(', found '+' | 13 |
| 16 | `reject` | `duration_unit_d` | `{ duration > 1d }` | `400` | the duration 1d is not valid: unit "d" is not supported; allowed units are ns, us, µs, ms, s, m, h | 13 |
| 17 | `reject` | `duration_unit_w` | `{ duration > 1w }` | `400` | the duration 1w is not valid: unit "w" is not supported; allowed units are ns, us, µs, ms, s, m, h | 13 |
| 18 | `reject` | `duration_unit_y` | `{ duration > 1y }` | `400` | the duration 1y is not valid: unit "y" is not supported; allowed units are ns, us, µs, ms, s, m, h | 13 |
| 19 | `reject` | `duration_unknown_unit` | `{ duration > 5x }` | `400` | the duration 5x is not valid: unknown unit "x"; allowed units are ns, us, µs, ms, s, m, h | 13 |
| 20 | `reject` | `empty` | *(the empty query)* | `400` | expected a spanset filter ('{') or '(' | 0 |
| 21 | `reject` | `eof_after_pipe` | `{ .a = 1 } \|` | `400` | expected a pipeline stage (a `{...}` spanset filter, count, sum, avg, min, max, select, by, or coalesce) | 12 |
| 22 | `reject` | `field_chain_over_limit` | `{ .a = 1 && .a = 1 && .a = 1 && .a = 1 && .a = 1 && .a = 1 && .a = 1 && .a = 1 && .a = 1 & … (650 bytes, in full in the corpus file)` | `400` | nested past the parser's depth limit | 639 |
| 23 | `reject` | `intrinsic_colon_space_before` | `{ span :id = "0a1b" }` | `400` | expected ':' immediately after the intrinsic scope keyword (a space is allowed AFTER the colon, but not before it), found whitespace before ':' | 7 |
| 24 | `reject` | `kind_invalid_keyword` | `{ kind = frobnicate }` | `400` | expected an intrinsic (name, duration, status, kind) or a scoped attribute (span., resource., or the unscoped . form), found identifier \"frobnicate\" | 9 |
| 25 | `reject` | `lone_amp` | `{ .a = 1 } & { .b = 2 }` | `400` | input left over after the end of the query | 11 |
| 26 | `reject` | `metrics_compare_float_topn` | `{ .a = 1 } \| compare({ .b = 2 }, 10.5)` | `400` | expected a whole number (compare(f, topN[, start, end])), found number \"10.5\" | 33 |
| 27 | `reject` | `metrics_compare_three_args` | `{ .a = 1 } \| compare({ .b = 2 }, 10, 1787900555000000000)` | `400` | expected ',', found ')' | 56 |
| 28 | `reject` | `missing_rbrace` | `{ .a = 1` | `400` | expected '}' | 8 |
| 29 | `reject` | `missing_value` | `{ .a = }` | `400` | expected a field, a literal, or '(', found '}' | 7 |
| 30 | `reject` | `select_empty` | `{} \| select()` | `400` | expected a field (select() requires at least one field), found ')' | 12 |
| 31 | `reject` | `spanset_chain_over_limit` | `{} \|\| {} \|\| {} \|\| {} \|\| {} \|\| {} \|\| {} \|\| {} \|\| {} \|\| {} \|\| {} \|\| {} \|\| {} \|\| {} \|\| {} \|\|  … (386 bytes, in full in the corpus file)` | `400` | nested past the parser's depth limit | 381 |
| 32 | `reject` | `status_invalid_keyword` | `{ status = bogus }` | `400` | expected an intrinsic (name, duration, status, kind) or a scoped attribute (span., resource., or the unscoped . form), found identifier \"bogus\" | 11 |
| 33 | `reject` | `string_escape_bad_hex` | `{ name = "\xZZ" }` | `400` | expected a valid escape (\\a, \\b, \\f, \\n, \\r, \\t, \\v, \\\\, \\\", \\xHH, \\NNN octal, \\uXXXX, or \\UXXXXXXXX), found 'Z' in a hex escape (expected 2 hex digits) | 10 |
| 34 | `reject` | `string_escape_non_ascii_byte` | `{ name = "\xff" }` | `400` | expected a byte escape at or below 0x7F (non-ASCII bytes are not representable as UTF-8 text; use \\uXXXX), found byte escape value 0xFF | 10 |
| 35 | `reject` | `string_escape_octal_out_of_range` | `{ name = "\777" }` | `400` | expected a byte escape at or below 0x7F (non-ASCII bytes are not representable as UTF-8 text; use \\uXXXX), found byte escape value 0x1FF | 10 |
| 36 | `reject` | `string_escape_surrogate` | `{ name = "\uD800" }` | `400` | expected a valid Unicode scalar value (not a surrogate, at most 0x10FFFF), found Unicode escape value 0xD800 | 10 |
| 37 | `reject` | `string_escape_unknown` | `{ name = "\z" }` | `400` | expected a valid escape (\\a, \\b, \\f, \\n, \\r, \\t, \\v, \\\\, \\\", \\xHH, \\NNN octal, \\uXXXX, or \\UXXXXXXXX), found escape sequence '\\z' | 10 |
| 38 | `reject` | `string_raw_newline` | `{ name = "line1\nline2" }` | `400` | a string literal with no closing quote | 9 |
| 39 | `reject` | `trailing_input` | `{ .a = 1 } { .b = 2 }` | `400` | input left over after the end of the query | 11 |
| 40 | `reject` | `unterminated_string` | `{ name = "abc` | `400` | a string literal with no closing quote | 9 |
| 41 | `unsupported` | `parent_scope` | `{ parent.foo = "x" }` | `400` | not yet supported: parent scope | 2 |
| 42 | `unsupported` | `structural_gte` | `{ .a = 1 } >= { .b = 2 }` | `400` | not yet supported: structural operator '>=' | 11 |
| 43 | `unsupported` | `structural_lte` | `{ .a = 1 } <= { .b = 2 }` | `400` | not yet supported: structural operator '<=' | 11 |
| 44 | `validate_reject` | `aggregate_non_numeric` | `{} \| avg(name) > 1` | `400` | aggregate field expressions must resolve to a number type: avg(name) | - |
| 45 | `validate_reject` | `bare_intrinsic_word` | `{ name }` | `400` | span filter field expressions must resolve to a boolean: { name } | - |
| 46 | `validate_reject` | `chained_comparison` | `{ .a = 1 = 2 }` | `400` | binary operations must operate on the same type: .a = 1 = 2 | - |
| 47 | `validate_reject` | `nested_set_regex_string` | `{ nestedSetLeft =~ "x" }` | `400` | binary operations must operate on the same type: nestedSetLeft =~ "x" | - |

## 3 the planner refuses

These parse and validate, so they sit under `accept/`, and the route answers
`400` all the same. The reason column is the rule
`docs/TraceQL/server-implementation.md` §3.2 states; the message column is what
the shipped planner itself returned when the probe of `measure/README.md` ran it
(`measure/planner_dispositions.tsv`).

| # | name | TraceQL | status | the rule | the shipped planner's message |
|---:|---|---|---|---|---|
| 1 | `by_expression_key` | `{ .a = 1 } \| by(.b + .c)` | `400` | a grouping key must resolve to a single per-span value, so it must be an attribute or an intrinsic | type mismatch: by((.b + .c)) is not a group key this engine can execute: a grouping key must resolve to a single per-span value, so it must be an attribute or an intrinsic |
| 2 | `duration_unitless` | `{ duration > 100 }` | `400` | duration requires a duration literal | type mismatch: duration requires a duration literal |
| 3 | `pipeline_spanset_operation` | `{ .a = 1 } \| { .b = 2 } && { .c = 3 }` | `400` | a `\|` stage must be a single { ... } filter, not a cross-spanset or structural operation | type mismatch: ({ .b = 2 } && { .c = 3 }) is not executable as a pipeline stage: a `\|` stage must be a single { ... } filter, not a cross-spanset or structural operation |
