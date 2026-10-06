// The ONE Rust implementation of the `[http.causes]` shape rule (M6-F2, SPEC §23.5.6). It has no
// dependencies on purpose: `registry.rs` (which validates the TOML on its way into the lock) and
// `build.rs` (which re-validates the LOCK before it generates a constant) both `include!` this file,
// so the two Rust readers cannot drift. `proto/tools/gen-php.php` carries the PHP copy, and the
// shared fixture `proto/tools/http-causes-shape-cases.json` is run against both languages
// (`registry_sync.rs` and `php/client`'s `HttpCausesShapeRuleTest`), so the third cannot drift
// either.

/// The shape rule for `[http.causes]`, so a cause can neither acquire two spellings nor fail to
/// become a constant: the table is NON-EMPTY; a token is a lowercase ASCII letter followed by
/// lowercase ASCII letters, digits and `_` (a leading digit would make the generated Rust
/// `const` an invalid identifier); its KEY is exactly the token upper-cased; and no token appears
/// twice (implied by the key rule, since keys are unique — checked anyway, so the rule does not
/// lean on that implication). Returns the first violation.
pub fn check_http_causes(
    causes: &std::collections::BTreeMap<String, String>,
) -> Result<(), String> {
    if causes.is_empty() {
        return Err("[http.causes] is empty".to_string());
    }
    let mut seen = std::collections::BTreeSet::new();
    for (key, token) in causes {
        let mut bytes = token.bytes();
        let well_formed = bytes.next().is_some_and(|b| b.is_ascii_lowercase())
            && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        if !well_formed {
            return Err(format!(
                "[http.causes] {key}: token {token:?} is not [a-z][a-z0-9_]*"
            ));
        }
        if *key != token.to_ascii_uppercase() {
            return Err(format!(
                "[http.causes] {key}: the key must be the token upper-cased ({})",
                token.to_ascii_uppercase()
            ));
        }
        if !seen.insert(token.as_str()) {
            return Err(format!("[http.causes] token {token:?} appears twice"));
        }
    }
    Ok(())
}
