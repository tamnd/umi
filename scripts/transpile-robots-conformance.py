"""Turn the upstream Google robots.txt conformance suite into a Rust test.

Reads robots_test.cc and emits one Rust #[test] per C++ TEST block, keeping
only the IsUserAgentAllowed assertions, which are the ones that test decision
semantics rather than Google's matcher API. The comment above each TEST block
comes across verbatim so the RFC citation travels with the case.
"""
import re, sys

src = open(sys.argv[1]).read()
lines = src.split('\n')

def rust_str(s):
    # s is the C++ literal contents, already unescaped into a Python str.
    out = s.replace('\\', '\\\\').replace('"', '\\"')
    out = out.replace('\n', '\\n').replace('\r', '\\r')
    return '"' + out + '"'

def unescape(cpp):
    return (cpp.replace('\\n', '\n').replace('\\r', '\r')
               .replace('\\"', '"').replace('\\\\', '\\'))

# Collect the string variables and the assertions per test, walking with a
# tiny brace-depth tracker so nested { } blocks in a TEST keep their own scope.
tests = []
i = 0
while i < len(lines):
    m = re.match(r'TEST\(RobotsUnittest, (\w+)\)', lines[i])
    if not m:
        i += 1
        continue
    name = m.group(1)
    # Grab the comment block directly above.
    doc = []
    j = i - 1
    while j >= 0 and lines[j].startswith('//'):
        doc.insert(0, lines[j][2:].strip())
        j -= 1
    # Body: to the matching closing brace at column 0.
    body = []
    k = i + 1
    while k < len(lines) and lines[k] != '}':
        body.append(lines[k])
        k += 1
    tests.append((name, doc, body))
    i = k + 1

def parse_body(body):
    """Yield ('let', var, value) and ('assert', expect, robots, agent, url)."""
    text = '\n'.join(l for l in body if not l.strip().startswith('//'))
    # Join C++ adjacent string literal concatenation and multi-line statements.
    stmts = []
    buf = ''
    depth = 0
    for ch in text:
        buf += ch
        if ch == ';':
            stmts.append(buf)
            buf = ''
    if buf.strip():
        stmts.append(buf)
    out = []
    for st in stmts:
        flat = ' '.join(st.split()).strip()
        flat = flat.lstrip('{} ').rstrip(';').strip()
        m = re.match(r'(?:const )?(?:absl::string_view|std::string) (\w+)\s*=\s*(.+)$', flat)
        if m:
            var, val = m.group(1), m.group(2)
            pieces = re.findall(r'"((?:[^"\\]|\\.)*)"', val)
            if pieces and val.strip().startswith('"'):
                out.append(('let', var, unescape(''.join(pieces))))
            continue
        m = re.match(r'EXPECT_(TRUE|FALSE)\(\s*IsUserAgentAllowed\((.*)\)\s*\)$', flat)
        if m:
            expect = m.group(1) == 'TRUE'
            args = split_args(m.group(2))
            if len(args) != 3:
                continue
            out.append(('assert', expect, *[lit(a) for a in args]))
            continue
        m = re.match(r'EXPECT_(TRUE|FALSE)\(IsUserAgentAllowed\((.*)\)\)$', flat)
        if m:
            expect = m.group(1) == 'TRUE'
            args = split_args(m.group(2))
            if len(args) == 3:
                out.append(('assert', expect, *[lit(a) for a in args]))
    return out

def split_args(s):
    args, depth, buf, instr = [], 0, '', False
    esc = False
    for ch in s:
        if instr:
            buf += ch
            if esc: esc = False
            elif ch == '\\': esc = True
            elif ch == '"': instr = False
            continue
        if ch == '"':
            instr = True; buf += ch; continue
        if ch in '([': depth += 1
        if ch in ')]': depth -= 1
        if ch == ',' and depth == 0:
            args.append(buf.strip()); buf = ''; continue
        buf += ch
    if buf.strip():
        args.append(buf.strip())
    return args

def lit(a):
    a = a.strip()
    if a.startswith('"'):
        pieces = re.findall(r'"((?:[^"\\]|\\.)*)"', a)
        return ('str', unescape(''.join(pieces)))
    if a.startswith('absl::StrCat('):
        parts = split_args(a[len('absl::StrCat('):-1])
        return ('cat', [lit(p) for p in parts])
    return ('var', a)

def emit_expr(v, indent='        '):
    kind = v[0]
    if kind == 'str':
        return rust_str(v[1])
    if kind == 'var':
        return v[1]
    if kind == 'cat':
        inner = ', '.join(
            (f'"{{{p[1]}}}"' if p[0] == 'var' else '') for p in v[1])
        fmt = ''
        args = []
        for p in v[1]:
            if p[0] == 'str':
                fmt += p[1].replace('{', '{{').replace('}', '}}')
            else:
                fmt += '{' + p[1] + '}'
        return 'format!(' + rust_str(fmt) + ')'
    raise SystemExit(f'unhandled {v}')

def snake(name):
    # `ID_` marks a case drawn straight from RFC 9309, `GoogleOnly_` marks one
    # of Google's extensions. Keeping that distinction in the test name is the
    # whole point, so it survives into readable Rust rather than `i_d_`.
    name = name.replace('ID_', 'rfc_').replace('GoogleOnly_', 'google_')
    for acronym, plain in (('HTMLis', 'Html_is'), ('REP', 'Rep'),
                           ('UTF8', 'Utf8'), ('URI', 'Uri')):
        name = name.replace(acronym, plain)
    s = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    return re.sub(r'_+', '_', s).strip('_')

emitted = []
count = 0
for name, doc, body in tests:
    ops = parse_body(body)
    asserts = [o for o in ops if o[0] == 'assert']
    if not asserts or name == 'GoogleOnly_LineTooLong':
        # LineTooLong builds its robots.txt with a C++ loop rather than a
        # literal, so it is written out by hand in the header instead.
        continue
    out = []
    for d in doc:
        # Plain comments, not doc comments. The upstream prose contains
        # unindented list items that rustdoc reads as lazy continuations, and
        # reflowing someone else's citation to please a lint is the wrong
        # trade.
        out.append(f'// {d}'.rstrip())
    out.append('#[test]')
    out.append(f'fn {snake(name)}() {{')
    for o in ops:
        if o[0] == 'let':
            out.append(f'    let {o[1]} = {rust_str(o[2])};')
        else:
            _, expect, rb, ua, url = o
            fn = 'assert!' if expect else 'assert!(!'
            call = f'allowed({emit_expr(rb)}, {emit_expr(ua)}, {emit_expr(url)})'
            if expect:
                out.append(f'    assert!({call});')
            else:
                out.append(f'    assert!(!{call});')
            count += 1
    out.append('}')
    emitted.append('\n'.join(out))

print(f'// {count} assertions in {len(emitted)} tests', file=sys.stderr)
open(sys.argv[2], 'w').write('\n\n'.join(emitted) + '\n')
