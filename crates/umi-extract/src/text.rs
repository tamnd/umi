//! Markdown to plain text.
//!
//! Doc 11.3 ends with the reason this exists: plain text is not stored, it is a
//! pure function of the markdown, and the function lives here so that every
//! consumer produces the same bytes we hashed in doc 11.7. That makes this a
//! published interface rather than an internal helper, and it means a change to
//! it is a major version bump under doc 11.10, because every content digest in
//! the corpus depends on it.
//!
//! The function is exactly "strip markup, collapse whitespace, NFC". Collapse
//! means all of it, so the result is one line with single spaces. That is on
//! purpose: it makes the digest indifferent to whether a template wrapped its
//! prose in paragraphs or in line breaks, which is the whole point of hashing
//! text rather than markup.

use unicode_normalization::UnicodeNormalization;

/// Strip markdown to the text a reader would see.
pub fn plain_text(markdown: &str) -> String {
    let mut out = String::with_capacity(markdown.len());
    let mut fence: Option<String> = None;

    for line in markdown.lines() {
        let trimmed = line.trim_start();

        if let Some(marker) = &fence {
            if trimmed.starts_with(marker.as_str()) {
                fence = None;
            } else {
                push_words(&mut out, line);
            }
            continue;
        }
        if let Some(marker) = opening_fence(trimmed) {
            fence = Some(marker);
            continue;
        }
        if is_rule(trimmed) || is_table_rule(trimmed) {
            continue;
        }

        let body = strip_prefixes(trimmed);
        push_words(&mut out, &strip_inline(body));
    }

    out.nfc().collect()
}

/// The fence a line opens, if it opens one. Only backticks, because the
/// serialiser only emits backticks.
fn opening_fence(line: &str) -> Option<String> {
    let ticks = line.chars().take_while(|&c| c == '`').count();
    (ticks >= 3).then(|| "`".repeat(ticks))
}

/// A horizontal rule, which carries no text. A paragraph that really did start
/// with three dashes arrives here as `\---` and does not match, which is what
/// the escape in the serialiser is for.
fn is_rule(line: &str) -> bool {
    let line = line.trim_end();
    line.len() >= 3 && line.chars().all(|c| c == '-')
}

/// The `| --- | --- |` row under a table header, which is markup and not data.
fn is_table_rule(line: &str) -> bool {
    let line = line.trim_end();
    line.starts_with('|')
        && line.len() > 1
        && line
            .chars()
            .all(|c| matches!(c, '|' | '-' | ':' | ' ' | '\t'))
}

/// Remove the block markers at the start of a line: quote arrows, heading
/// hashes and list bullets, in any order and any number, because a quoted list
/// inside a quote is a real thing.
fn strip_prefixes(line: &str) -> &str {
    let mut rest = line;
    loop {
        let start = rest;
        rest = rest.trim_start();
        if let Some(after) = rest.strip_prefix('>') {
            rest = after;
        } else if rest.starts_with('#') {
            let hashes = rest.chars().take_while(|&c| c == '#').count();
            if hashes <= 6 && rest[hashes..].starts_with(' ') {
                rest = &rest[hashes..];
            }
        } else if let Some(after) = strip_bullet(rest) {
            rest = after;
        }
        if rest == start {
            return rest;
        }
    }
}

/// A `- ` or `1. ` list marker.
fn strip_bullet(line: &str) -> Option<&str> {
    if let Some(after) = line.strip_prefix("- ") {
        return Some(after);
    }
    let digits = line.chars().take_while(char::is_ascii_digit).count();
    if digits == 0 {
        return None;
    }
    let rest = &line[digits..];
    rest.strip_prefix(". ").or_else(|| rest.strip_prefix(") "))
}

/// Remove inline markup: escapes, emphasis, code spans, link destinations and
/// table pipes.
fn strip_inline(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            // A backslash escape means the next character is literal, which is
            // exactly the case where it must not be read as markup.
            '\\' => {
                if let Some(next) = chars.next() {
                    out.push(next);
                }
            }
            '`' => {
                while chars.peek() == Some(&'`') {
                    chars.next();
                }
            }
            '*' | '_' | '|' => {}
            '[' => {}
            ']' => {
                // `](url)` is a link or an image. The text is already out, the
                // destination is not text.
                if chars.peek() == Some(&'(') {
                    chars.next();
                    let mut depth = 1usize;
                    while let Some(next) = chars.next() {
                        match next {
                            // The serialiser escapes parentheses inside a URL,
                            // so a backslash here hides the character after it
                            // from the depth count.
                            '\\' => {
                                chars.next();
                            }
                            '(' => depth += 1,
                            ')' => {
                                depth -= 1;
                                if depth == 0 {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                } else {
                    out.push(']');
                }
            }
            _ => out.push(ch),
        }
    }
    out
}

/// Append a line's words to the output with single spaces between everything.
fn push_words(out: &mut String, line: &str) {
    for word in line.split_ascii_whitespace() {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(word);
    }
}
