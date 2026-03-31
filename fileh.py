import re
from pathlib import Path

input_path = Path(r"C:\Users\lmathope\Downloads\emails_rows_no_filebinary.sql")
output_path = Path(r"C:\Users\lmathope\Desktop\Dev\25\New folder\bestmed\emails_rows_cleaned.sql")

def read_text(path: Path) -> str:
    with path.open("r", encoding="utf-8", errors="replace") as f:
        return f.read()

def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write(text)

def remove_filebinary(sql_text: str) -> str:
    cleaned_sql = sql_text

    # Pass 1: Simple removal (with optional leading comma)
    cleaned_sql = re.sub(
        r',?\s*\\\\"fileBinary\\\\":\s*\\\\"(?:[^"\\]|\\.)*\\\\"',
        '',
        cleaned_sql,
        flags=re.DOTALL
    )

    # Pass 2: Aggressive until next known key or closing
    cleaned_sql = re.sub(
        r'\\\\"fileBinary\\\\":\s*\\\\".*?(?=\\\\"(?:member|schemeCode|correspondenceDate|expiryDate|provider|barcode|documentType|documentSubType|payer|intermediary|source|encoding|carbonCopy|physicalLocation|thirdParty|dependantNo|entityType|fileName)\\\\"|}")',
        '',
        cleaned_sql,
        flags=re.DOTALL
    )

    # Pass 3: fileBinary at end of JSON object
    cleaned_sql = re.sub(
        r',?\s*\\\\"fileBinary\\\\":\s*\\\\".*?\\\\"\s*(?=})',
        '',
        cleaned_sql,
        flags=re.DOTALL
    )

    return cleaned_sql

def tidy_json_separators(sql_text: str) -> str:
    cleaned_sql = sql_text
    # Remove multiple commas
    cleaned_sql = re.sub(r',\s*,+', ',', cleaned_sql)
    # Remove comma before closing brace
    cleaned_sql = re.sub(r',\s*}', '}', cleaned_sql)
    # Remove comma before next field
    cleaned_sql = re.sub(r',(\s*\\\\"[a-zA-Z])', r'\1', cleaned_sql)
    # Remove leading comma after opening brace
    cleaned_sql = re.sub(r'{\s*,', '{', cleaned_sql)
    return cleaned_sql

def _read_parenthesized(s: str, i: int):
    # expects s[i] == '(' ; returns (segment, next_index)
    j = i
    depth = 0
    in_single = False
    in_double = False
    while j < len(s):
        ch = s[j]
        if in_single:
            if ch == "'":
                # Handle doubled single-quote escape: ''
                if j + 1 < len(s) and s[j + 1] == "'":
                    j += 2
                    continue
                in_single = False
            j += 1
            continue
        if in_double:
            if ch == '"':
                # Handle doubled double-quote escape: ""
                if j + 1 < len(s) and s[j + 1] == '"':
                    j += 2
                    continue
                in_double = False
            j += 1
            continue

        if ch == "'":
            in_single = True
        elif ch == '"':
            in_double = True
        elif ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth == 0:
                j += 1
                return s[i:j], j
        j += 1
    return s[i:], j

def _split_values_row(inner: str):
    parts = []
    buf = []
    i = 0
    n = len(inner)
    in_single = False
    in_double = False
    brace = bracket = 0
    while i < n:
        ch = inner[i]
        if in_single:
            buf.append(ch)
            if ch == "'":
                if i + 1 < n and inner[i + 1] == "'":
                    buf.append(inner[i + 1])
                    i += 2
                    continue
                in_single = False
            i += 1
            continue
        if in_double:
            buf.append(ch)
            if ch == '"':
                if i + 1 < n and inner[i + 1] == '"':
                    buf.append(inner[i + 1])
                    i += 2
                    continue
                in_double = False
            i += 1
            continue

        if ch == "'":
            in_single = True
            buf.append(ch)
            i += 1
            continue
        if ch == '"':
            in_double = True
            buf.append(ch)
            i += 1
            continue
        if ch == '{':
            brace += 1
        elif ch == '}':
            brace = max(0, brace - 1)
        elif ch == '[':
            bracket += 1
        elif ch == ']':
            bracket = max(0, bracket - 1)

        if ch == ',' and brace == 0 and bracket == 0:
            parts.append(''.join(buf))
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    parts.append(''.join(buf))
    return parts

def _is_word_boundary(s: str, i: int) -> bool:
    if i <= 0:
        return True
    return not (s[i-1].isalnum() or s[i-1] == '_')

def _replace_12th_value_in_values(sql_text: str) -> str:
    out = []
    i = 0
    n = len(sql_text)
    in_single = False
    in_double = False

    while i < n:
        ch = sql_text[i]

        # Track string contexts for top-level scanning
        if in_single:
            out.append(ch)
            if ch == "'":
                if i + 1 < n and sql_text[i + 1] == "'":
                    out.append(sql_text[i + 1])
                    i += 2
                    continue
                in_single = False
            i += 1
            continue
        if in_double:
            out.append(ch)
            if ch == '"':
                if i + 1 < n and sql_text[i + 1] == '"':
                    out.append(sql_text[i + 1])
                    i += 2
                    continue
                in_double = False
            i += 1
            continue

        if ch == "'":
            in_single = True
            out.append(ch)
            i += 1
            continue
        if ch == '"':
            in_double = True
            out.append(ch)
            i += 1
            continue

        # Detect VALUES keyword at top-level (case-insensitive)
        if sql_text[i:i+6].lower() == 'values' and _is_word_boundary(sql_text, i):
            out.append(sql_text[i:i+6])
            i += 6
            # whitespace after VALUES
            while i < n and sql_text[i].isspace():
                out.append(sql_text[i]); i += 1

            # Process one or more tuples: (...), (...), ... ;
            while i < n:
                if sql_text[i] == '(':
                    group, i = _read_parenthesized(sql_text, i)
                    inner = group[1:-1]
                    vals = _split_values_row(inner)
                    if len(vals) >= 12:
                        # Replace 12th value with NULL
                        val12 = vals[11]
                        m_lead = re.match(r'\s*', val12)
                        trail_ws = ''
                        lead_ws = m_lead.group(0) if m_lead else ''
                        vals[11] = f"{lead_ws}NULL"
                    out.append('(' + ','.join(vals) + ')')

                    # Copy trailing whitespace
                    while i < n and sql_text[i].isspace():
                        out.append(sql_text[i]); i += 1

                    # Next tuple?
                    if i < n and sql_text[i] == ',':
                        out.append(sql_text[i]); i += 1
                        while i < n and sql_text[i].isspace():
                            out.append(sql_text[i]); i += 1
                        continue
                # Stop at semicolon or any non-tuple char
                if i < n:
                    out.append(sql_text[i])
                    if sql_text[i] == ';':
                        i += 1
                        break
                    i += 1
                else:
                    break
            continue

        out.append(ch)
        i += 1

    return ''.join(out)
def main():
    sql_text = read_text(input_path)
    print(f"Processing: {input_path}")
    print(f"Original size: {len(sql_text):,} chars")

    cleaned = remove_filebinary(sql_text)
    cleaned = tidy_json_separators(cleaned)
    cleaned = _replace_12th_value_in_values(cleaned)

    write_text(output_path, cleaned)
    print(f"Saved: {output_path}")
    print(f"Cleaned size: {len(cleaned):,} chars")
    diff = len(sql_text) - len(cleaned)
    pct = (diff / len(sql_text) * 100) if len(sql_text) else 0
    print(f"Reduced by: {diff:,} chars ({pct:.2f}%)")

    remaining_fb = len(re.findall(r'\\\\"fileBinary\\\\"', cleaned))
    print(f"Remaining fileBinary occurrences: {remaining_fb}")

if __name__ == "__main__":
    main()