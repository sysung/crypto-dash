import re
from typing import Tuple


def is_query_safe(sql: str) -> Tuple[bool, str]:
    """
    Validates that the generated SQL query is read-only and safe for execution.
    Strips single-line and multi-line SQL comments and checks for forbidden keywords.
    """
    cleaned_sql = re.sub(r"--.*", "", sql)
    cleaned_sql = re.sub(r"/\*.*?\*/", "", cleaned_sql, flags=re.DOTALL)
    cleaned_sql = cleaned_sql.strip()

    if not cleaned_sql:
        return False, "Query is empty."

    upper_sql = cleaned_sql.upper()

    if not (upper_sql.startswith("SELECT") or upper_sql.startswith("WITH")):
        return False, "Query must start with SELECT or WITH."

    forbidden_patterns = [
        r"\bDROP\b",
        r"\bALTER\b",
        r"\bTRUNCATE\b",
        r"\bINSERT\b",
        r"\bUPDATE\b",
        r"\bDELETE\b",
        r"\bCREATE\b",
        r"\bREPLACE\b"
    ]

    for pattern in forbidden_patterns:
        if re.search(pattern, upper_sql):
            keyword = pattern.replace(r"\b", "")
            return False, f"Forbidden SQL keyword detected: {keyword}"

    return True, ""
