"""SQL template rendering with Jinja2-style parameter substitution."""

import re
from pathlib import Path
from typing import Any


def render_sql_template(sql_path: str, params: dict[str, Any]) -> str:
    """Render a SQL template file by substituting {{ param }} placeholders.

    Args:
        sql_path: Path to the SQL template file
        params: Dictionary of parameter name -> value

    Returns:
        Rendered SQL string with all parameters substituted
    """
    sql_text = Path(sql_path).read_text()

    for key, value in params.items():
        pattern = r"\{\{\s*" + re.escape(key) + r"\s*\}\}"
        sql_text = re.sub(pattern, str(value), sql_text)

    # Check for unresolved placeholders
    unresolved = re.findall(r"\{\{\s*(\w+)\s*\}\}", sql_text)
    if unresolved:
        raise ValueError(
            f"Unresolved SQL template parameters: {', '.join(set(unresolved))}"
        )

    return sql_text
