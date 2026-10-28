//! HTML helpers for the admin pages.

/// Step-1 stub: returns its input.
pub(crate) fn html_text(s: &str) -> String {
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// B9: every character that ends text or an attribute is escaped.
    #[test]
    fn html_text_escapes_the_five_markup_characters() {
        assert_eq!(
            html_text("<a href='x'>&\""),
            "&lt;a href=&#39;x&#39;&gt;&amp;&quot;"
        );
    }
}
