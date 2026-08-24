# AT\&T, 5 \< 6, and \*asterisks\*

Prose is full of characters that a markdown parser wants to read as markup. Underscores in snake\_case identifiers, asterisks used as \*emphasis by hand\*, square brackets in \[citation needed\], backticks in \`quoted\` shell commands, and ampersands in AT\&T all have to survive a round trip through the serialiser.

“Curly quotes” and an em dash — which we do not write ourselves but which pages are full of — are just characters and pass straight through.

\- A line that begins with a dash but is not a list.

\1. A line that begins with a number and a dot but is not a list either.

\# Not a heading.

\> Not a quote.

A non breaking space is not ASCII whitespace and does not collapse.