//! HTML helpers for the admin pages.

/// `s` as HTML text: `&`, `<`, `>`, `"` and `'` escaped, so it is safe in
/// an element and in a quoted attribute.
pub(crate) fn html_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}

/// A whole admin page: the stylesheet, links to the three pages, `title`
/// as the heading, then `body`, which the caller has escaped.
pub(crate) fn layout(title: &str, body: &str) -> String {
    let title = html_text(title);
    format!(
        "<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\
         <title>PulsusDB admin: {title}</title>\
         <link rel=\"stylesheet\" href=\"/admin/admin.css\"></head><body>\
         <nav><a href=\"/admin/\">Status</a> <a href=\"/admin/database\">Database</a> \
         <a href=\"/admin/config\">Configuration</a></nav>\
         <main><h1>{title}</h1>{body}</main></body></html>\n"
    )
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
