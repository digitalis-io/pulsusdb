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

/// The four admin pages, as (path, name); a page's title is its name.
const PAGES: [(&str, &str); 4] = [
    ("/admin/", "Status"),
    ("/admin/database", "Database"),
    ("/admin/config", "Configuration"),
    ("/admin/setup", "Setup"),
];

/// A whole admin page: the stylesheet, a header bar linking the four pages
/// with the one titled `title` marked current, `title` as the heading,
/// then `body`, which the caller has escaped.
pub(crate) fn layout(title: &str, body: &str) -> String {
    let mut nav = String::new();
    for (path, name) in PAGES {
        let current = if name == title {
            " aria-current=\"page\""
        } else {
            ""
        };
        nav.push_str(&format!("<a href=\"{path}\"{current}>{name}</a>"));
    }
    let title = html_text(title);
    format!(
        "<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <title>PulsusDB admin: {title}</title>\
         <link rel=\"stylesheet\" href=\"/admin/admin.css\"></head><body>\
         <header class=\"topbar\"><div class=\"topbar-inner\">\
         <a class=\"brand\" href=\"/admin/\">PulsusDB <span>admin</span></a>\
         <nav>{nav}</nav></div></header>\
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

    /// The header bar marks the page being shown, and only that one.
    #[test]
    fn layout_marks_the_current_page() {
        let page = layout("Database", "");
        assert!(
            page.contains("<a href=\"/admin/database\" aria-current=\"page\">Database</a>"),
            "{page}"
        );
        assert_eq!(page.matches("aria-current").count(), 1, "{page}");
    }
}
