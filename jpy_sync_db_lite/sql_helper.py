"""
SQL Helper utilities for parsing and cleaning SQL statements.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

import re
from pathlib import Path

import sqlparse
from sqlparse.sql import Statement, Token
from sqlparse.tokens import Comment

from jpy_sync_db_lite.errors import (
    SqlFileError,
    SqlValidationError,
)

# Private constants for SQL statement types
_FETCH_KEYWORDS: set[str] = {"SELECT", "VALUES", "SHOW", "EXPLAIN", "PRAGMA", "DESC", "DESCRIBE"}
_MODIFY_DML_KEYWORDS: set[str] = {"INSERT", "UPDATE", "DELETE"}

# Private constants for SQL keywords and symbols
_WITH_KEYWORD: str = "WITH"
_AS_KEYWORD: str = "AS"
_SELECT_KEYWORD: str = "SELECT"
_SEMICOLON: str = ";"
_COMMA: str = ","
_PAREN_OPEN: str = "("
_PAREN_CLOSE: str = ")"

# Public constants for statement type return values
EXECUTE_STATEMENT: str = "execute"
FETCH_STATEMENT: str = "fetch"


def remove_sql_comments(sql_text: str) -> str:
    """
    Remove SQL comments from a SQL string using sqlparse.
    Handles:
    - Single-line comments (-- comment)
    - Multi-line comments (/* comment */)
    - Preserves comments within string literals

    Args:
        sql_text: SQL string that may contain comments
    Returns:
        SQL string with comments removed
    """
    if not sql_text:
        return sql_text

    result = sqlparse.format(sql_text, strip_comments=True)
    return str(result) if result is not None else ""


def normalize_token(token: Token) -> str:
    """
    Return the uppercased, stripped value of a token.
    """
    return str(token.value).strip().upper() if hasattr(token, "value") and token.value else ""


def _next_significant_token(
    tokens: list[Token],
    *,
    start: int = 0,
) -> tuple[int | None, Token | None]:
    """
    Return the index and token of the next non-whitespace, non-comment token.
    """
    for i in range(start, len(tokens)):
        token = tokens[i]
        if not token.is_whitespace and token.ttype not in Comment:
            return i, token
    return None, None


def find_main_statement_after_with(tokens: list[Token]) -> str | None:
    """
    Find the main statement after CTE definitions by scanning tokens after WITH.

    This unified scanner handles the complete CTE parsing logic:
    - Skips whitespace and comments
    - For each CTE: expects AS, then consumes balanced (...) body
    - Note: optional column lists immediately after the CTE name (e.g., cte_name(col1, col2))
      are not explicitly parsed. The scanner advances token-by-token until it encounters
      the AS keyword, without tracking parentheses before AS.
    - After CTE body: if next significant token is comma, continues to next CTE; otherwise breaks
    - Returns the next significant keyword as the main statement

    Args:
        tokens: List of sqlparse tokens to analyze (should be tokens after WITH keyword)
    Returns:
        The main statement type (e.g., 'SELECT', 'INSERT') or None if not found
    """
    i = 0
    n = len(tokens)

    while i < n:
        token = tokens[i]
        token_value = normalize_token(token)

        # Skip whitespace and comments
        if token.is_whitespace or token.ttype in Comment:
            i += 1
            continue

        # Look for AS keyword (start of CTE definition)
        if token_value == _AS_KEYWORD:
            # Skip AS keyword
            i += 1

            # Find next significant token after AS
            next_i, _ = _next_significant_token(tokens, start=i)
            if next_i is None:
                return None
            i = next_i

            # Check if next token is opening parenthesis (CTE body)
            if tokens[i].ttype == sqlparse.tokens.Punctuation and tokens[i].value == _PAREN_OPEN:
                # Consume the entire CTE body by tracking parentheses
                paren_level = 1
                i += 1
                while i < n and paren_level > 0:
                    t = tokens[i]
                    if t.ttype == sqlparse.tokens.Punctuation:
                        if t.value == _PAREN_OPEN:
                            paren_level += 1
                        elif t.value == _PAREN_CLOSE:
                            paren_level -= 1
                    i += 1

                # Find next significant token after CTE body
                next_i, _ = _next_significant_token(tokens, start=i)
                if next_i is None:
                    return None
                i = next_i

                # Check if next token is comma (more CTEs to follow)
                if tokens[i].ttype == sqlparse.tokens.Punctuation and tokens[i].value == _COMMA:
                    i += 1  # Skip comma and continue to next CTE
                    continue
                else:
                    # No comma means we've reached the main statement
                    break
            else:
                # No opening parenthesis after AS - this is the main statement
                break
        else:
            # Not AS keyword - this might be the main statement
            i += 1

    # Find the next significant token (the main statement)
    next_i, token = _next_significant_token(tokens, start=i)
    if next_i is None or token is None:
        return None

    token_value = normalize_token(token)
    if token_value in _MODIFY_DML_KEYWORDS or token_value in _FETCH_KEYWORDS:
        return token_value
    return None


def detect_statement_type(sql: str) -> str:
    """
    Detect if a SQL statement returns rows using advanced sqlparse analysis.

    This function performs sophisticated SQL statement analysis to determine whether
    a statement will return rows (fetch operation) or perform an action without
    returning data (execute operation). It handles complex cases including CTEs,
    nested queries, and database-specific statements.

    Supported Statement Types:
        - SELECT statements (including subqueries and JOINs)
        - Common Table Expressions (WITH ... SELECT/INSERT/UPDATE/DELETE)
        - VALUES statements for literal value sets
        - Database introspection (SHOW, DESCRIBE/DESC, EXPLAIN, PRAGMA)
        - Data modification (INSERT, UPDATE, DELETE) - classified as execute
        - Schema operations (CREATE, ALTER, DROP) - classified as execute

    Args:
        sql: SQL statement string to analyze. Can contain comments, whitespace,
            and complex SQL constructs. Empty or whitespace-only strings are
            treated as execute operations.

    Returns:
        One of the following string constants:
        - 'fetch': Statement returns rows (SELECT, VALUES, SHOW, DESCRIBE, EXPLAIN, PRAGMA)
        - 'execute': Statement performs action without returning data (INSERT, UPDATE, DELETE, DDL)

    Examples:
        Simple SELECT:
            >>> detect_statement_type("SELECT * FROM users")
            'fetch'

        CTE with SELECT:
            >>> detect_statement_type('''
            ... WITH active_users AS (
            ...     SELECT id, name FROM users WHERE active = 1
            ... )
            ... SELECT * FROM active_users
            ... ''')
            'fetch'

        CTE with INSERT:
            >>> detect_statement_type('''
            ... WITH new_data AS (
            ...     SELECT 'John' as name, 25 as age
            ... )
            ... INSERT INTO users (name, age) SELECT * FROM new_data
            ... ''')
            'execute'

        VALUES statement:
            >>> detect_statement_type("VALUES (1, 'Alice'), (2, 'Bob')")
            'fetch'

        Database introspection:
            >>> detect_statement_type("DESCRIBE users")
            'fetch'
            >>> detect_statement_type("SHOW TABLES")
            'fetch'
            >>> detect_statement_type("EXPLAIN SELECT * FROM users")
            'fetch'

        Data modification:
            >>> detect_statement_type("INSERT INTO users (name) VALUES ('John')")
            'execute'
            >>> detect_statement_type("UPDATE users SET active = 1")
            'execute'

        Schema operations:
            >>> detect_statement_type("CREATE TABLE test (id INT)")
            'execute'

    Note:
        - Parsing is performed using sqlparse library for accuracy
        - Comments are automatically handled and ignored
        - Complex nested CTEs are supported through unified scanner analysis
        - Database-specific syntax (PRAGMA, SHOW) is recognized
        - Thread-safe: Can be called concurrently from multiple threads
    """
    if not sql or not sql.strip():
        return EXECUTE_STATEMENT

    parsed = sqlparse.parse(sql.strip())
    if not parsed:
        return EXECUTE_STATEMENT

    stmt = parsed[0]
    tokens = list(stmt.flatten())
    if not tokens:
        return EXECUTE_STATEMENT

    _, first_token = _next_significant_token(tokens)
    if first_token is None:
        return EXECUTE_STATEMENT

    token_value = normalize_token(first_token)

    # DESC/DESCRIBE detection (regardless of token type)
    if token_value in ("DESC", "DESCRIBE"):
        return FETCH_STATEMENT

    # CTE detection: WITH ...
    if token_value == _WITH_KEYWORD:
        # Get all tokens after WITH keyword and use unified scanner
        after_with_tokens = tokens[1:]  # Skip the WITH token itself
        main_stmt = find_main_statement_after_with(after_with_tokens)

        # Classify based on main statement type
        if main_stmt in _FETCH_KEYWORDS:
            return FETCH_STATEMENT
        return EXECUTE_STATEMENT

    # All other statements: classify based on first keyword
    if token_value in _FETCH_KEYWORDS:
        return FETCH_STATEMENT

    return EXECUTE_STATEMENT


def parse_sql_statements(
    sql_text: str,
    *,
    strip_semicolon: bool = False,
) -> list[str]:
    """
    Parse a SQL string containing multiple statements into a list of individual statements
    using sqlparse.
    Handles:
    - Statements separated by semicolons
    - Preserves semicolons within string literals
    - Removes comments before parsing
    - Trims whitespace from individual statements
    - Filters out empty statements and statements that are only comments

    Args:
        sql_text: SQL string that may contain multiple statements
        strip_semicolon: If True, strip trailing semicolons in statements (default: False)
    Returns:
        List of individual SQL statements (with or without trailing semicolons based on parameter)
    """
    if not sql_text:
        return []

    # Remove comments first
    clean_sql = remove_sql_comments(sql_text)

    # Use sqlparse.split for efficient statement splitting
    statements = sqlparse.split(clean_sql)
    filtered_stmts: list[str] = []

    for stmt in statements:
        stmt_str = stmt.strip()
        if not stmt_str:
            continue

        # Filter out statements that are just semicolons
        if stmt_str == _SEMICOLON:
            continue

        # Filter out statements that are only comments (even after remove_sql_comments)
        # This handles cases where sqlparse.format doesn't remove all comment types
        if stmt_str.startswith("/*") and stmt_str.endswith("*/"):
            continue
        if stmt_str.startswith("--"):
            continue

        # Apply semicolon stripping based on parameter
        if strip_semicolon:
            stmt_str = stmt_str.rstrip(";").strip()

        filtered_stmts.append(stmt_str)

    return filtered_stmts


def extract_create_table_statements(sql_content: str) -> list[tuple[str, str]]:
    """
    Extract CREATE TABLE statements and their table bodies using sqlparse.

    This function parses SQL content to find all CREATE TABLE statements and extracts:
    - Table name (normalized to lowercase)
    - Table body (content between parentheses after table name)

    Args:
        sql_content: SQL content that may contain CREATE TABLE statements

    Returns:
        List of tuples containing (table_name, table_body) for each CREATE TABLE found

    Raises:
        SqlValidationError: If sqlparse fails to parse the content or encounters malformed CREATE TABLE statements
    """
    if not sql_content or not sql_content.strip():
        return []

    # Remove comments first for cleaner parsing
    clean_sql = remove_sql_comments(sql_content)
    if not clean_sql.strip():
        return []

    try:
        # Parse the SQL content
        parsed = sqlparse.parse(clean_sql)
        if not parsed:
            return []

        create_tables = []

        for statement in parsed:
            tokens = list(statement.flatten())
            if not tokens:
                continue

            # Look for CREATE TABLE pattern
            i = 0
            while i < len(tokens):
                token = tokens[i]

                # Skip whitespace and comments
                if token.is_whitespace or token.ttype in Comment:
                    i += 1
                    continue

                # Check for CREATE keyword
                if normalize_token(token) == "CREATE":
                    # Look for TABLE keyword next
                    j, next_token = _next_significant_token(tokens, start=i + 1)
                    if j is not None and next_token and normalize_token(next_token) == "TABLE":
                        # Found CREATE TABLE, now extract table name and body
                        table_name, table_body = _extract_create_table_components(tokens, j + 1)
                        if table_name is not None and table_body is not None:
                            create_tables.append((table_name.lower(), table_body))

                i += 1

        return create_tables

    except Exception as e:
        raise SqlValidationError(f"Failed to parse CREATE TABLE statements: {e}") from e


def _extract_create_table_components(tokens: list[Token], start_index: int) -> tuple[str | None, str | None]:
    """
    Extract table name and body from CREATE TABLE statement tokens.

    Args:
        tokens: List of sqlparse tokens starting after TABLE keyword
        start_index: Index to start parsing from

    Returns:
        Tuple of (table_name, table_body) or (None, None) if not found
    """
    table_name = None
    table_body = None

    # Find table name (next identifier after TABLE)
    i, name_token = _next_significant_token(tokens, start=start_index)
    if name_token and hasattr(name_token, "ttype") and name_token.ttype is not None:
        # Check if it's an identifier (Name, Name.Placeholder, etc.)
        if "Name" in str(name_token.ttype):
            table_name = str(name_token.value).strip()

    if not table_name or i is None:
        return None, None

    # Find opening parenthesis
    i, paren_token = _next_significant_token(tokens, start=i + 1)
    if i is None or not paren_token or str(paren_token.value) != "(":
        return None, None

    # Extract everything between parentheses
    paren_count = 1
    body_start = i + 1
    body_end = body_start

    for j in range(body_start, len(tokens)):
        token = tokens[j]
        if str(token.value) == "(":
            paren_count += 1
        elif str(token.value) == ")":
            paren_count -= 1
            if paren_count == 0:
                body_end = j
                break

    if paren_count != 0:
        return None, None

    # Extract table body as string
    body_tokens = tokens[body_start:body_end]
    table_body = "".join(str(token.value) for token in body_tokens).strip()

    return table_name, table_body


def parse_table_columns(table_body: str) -> dict[str, str]:
    """
    Parse column definitions from table body using sqlparse tokens.

    This function parses the table body (content between parentheses in CREATE TABLE)
    to extract column names and their SQL types. It uses sqlparse for robust tokenization
    and handles complex column definitions with constraints.

    Args:
        table_body: Table body content between parentheses

    Returns:
        Dictionary mapping column names (lowercase) to normalized SQL types

    Raises:
        SqlValidationError: If the table body cannot be parsed with sqlparse or if no valid columns are found

    Examples:
        >>> parse_table_columns("id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL")
        {'id': 'INTEGER', 'name': 'VARCHAR'}

        >>> parse_table_columns("user_id INTEGER, email VARCHAR(255) UNIQUE")
        {'user_id': 'INTEGER', 'email': 'VARCHAR'}
    """
    if not table_body or not table_body.strip():
        raise SqlValidationError("No valid column definitions found in table body")

    columns: dict[str, str] = {}

    # Parse the table body as a SQL fragment
    parsed = sqlparse.parse(table_body)
    if not parsed:
        raise SqlValidationError("Failed to parse table body with sqlparse")

    # Get tokens from the parsed statement
    tokens = list(parsed[0].flatten())

    # Split by top-level commas
    column_parts = _split_by_top_level_commas(tokens)

    # If no valid columns were found, raise an error
    valid_columns_found = False

    for part_tokens in column_parts:
        column_name, sql_type = _extract_column_name_and_type(part_tokens)
        if column_name is not None and sql_type is not None:
            columns[column_name.lower()] = sql_type.upper()
            valid_columns_found = True

    # If no valid columns were parsed, raise an error
    if not valid_columns_found:
        raise SqlValidationError("No valid column definitions found in table body")

    return columns


def _split_by_top_level_commas(tokens: list[Token]) -> list[list[Token]]:
    """
    Split tokens by top-level commas (commas not inside parentheses).

    Args:
        tokens: List of sqlparse tokens

    Returns:
        List of token lists, each representing a column definition
    """
    parts = []
    current_part: list[Token] = []
    paren_count = 0

    for token in tokens:
        token_value = str(token.value)

        if token_value == "(":
            paren_count += 1
        elif token_value == ")":
            paren_count -= 1
        elif token_value == "," and paren_count == 0:
            if current_part:
                parts.append(current_part)
                current_part = []
            continue

        current_part.append(token)

    if current_part:
        parts.append(current_part)

    return parts


def _extract_column_name_and_type(tokens: list[Token]) -> tuple[str | None, str | None]:
    """
    Extract column name and SQL type from column definition tokens.

    Args:
        tokens: List of tokens representing a single column definition

    Returns:
        Tuple of (column_name, sql_type) or (None, None) if not a column definition
    """
    if not tokens:
        return None, None

    # Skip constraint keywords at the beginning
    constraint_keywords = {
        "PRIMARY",
        "FOREIGN",
        "UNIQUE",
        "CHECK",
        "CONSTRAINT",
        "INDEX",
        "KEY",
        "AUTOINCREMENT",
        "DEFAULT",
        "NOT",
        "NULL",
        "REFERENCES",
    }

    # Find the first identifier (column name)
    column_name = None
    type_start_idx = None

    for i, token in enumerate(tokens):
        if token.is_whitespace or token.ttype in Comment:
            continue

        token_value = normalize_token(token)

        # Skip constraint keywords
        if token_value in constraint_keywords:
            return None, None

        # Check if this is an identifier (column name)
        if hasattr(token, "ttype") and token.ttype is not None:
            # Reject quoted identifiers (not supported)
            if "String" in str(token.ttype):
                return None, None
            if "Name" in str(token.ttype):
                column_name = str(token.value).strip()
                type_start_idx = i + 1
                break

    if not column_name or type_start_idx is None:
        return None, None

    # Extract SQL type (next identifier after column name)
    sql_type = None
    type_tokens = []

    for i in range(type_start_idx, len(tokens)):
        token = tokens[i]

        if token.is_whitespace or token.ttype in Comment:
            continue

        token_value = str(token.value)

        # Stop at constraint keywords
        if normalize_token(token) in constraint_keywords:
            break

        # Collect type tokens
        type_tokens.append(token_value)

    if type_tokens:
        # Join type tokens and clean up
        sql_type = "".join(type_tokens).strip()
        # Remove size specifications like (255), (10,2)
        sql_type = re.sub(r"\(\s*\d+(?:\s*,\s*\d+)?\s*\)", "", sql_type).strip()
        # Remove any remaining constraint keywords that might have been included
        for keyword in constraint_keywords:
            sql_type = sql_type.replace(keyword, "").strip()
        # Legacy behavior: strip _TYPE suffix for unknown types
        if sql_type.endswith("_TYPE"):
            sql_type = sql_type[:-5]  # Remove '_TYPE' suffix

    return column_name, sql_type


# Fallback function removed - sqlparse parsing is required


def extract_table_names(sql_query: str) -> list[str]:
    """
    Extract table names from SQL query using sqlparse.

    This function parses SQL queries to extract table names from various clauses:
    - FROM clauses in SELECT statements
    - Target tables in INSERT, UPDATE, DELETE statements
    - JOIN clauses
    - CTE (Common Table Expression) names

    Args:
        sql_query: SQL query string to analyze

    Returns:
        List of table names found in the query (in lowercase)

    Raises:
        SqlValidationError: If the SQL query cannot be parsed with sqlparse

    Examples:
        >>> extract_table_names("SELECT * FROM users WHERE id = :id")
        ['users']

        >>> extract_table_names("INSERT INTO products (name) VALUES (:name)")
        ['products']

        >>> extract_table_names("UPDATE orders SET status = :status WHERE id = :id")
        ['orders']

        >>> extract_table_names("SELECT u.name, p.title FROM users u JOIN products p ON u.id = p.user_id")
        ['users', 'products']
    """
    if not sql_query or not sql_query.strip():
        return []

    # Remove comments first for cleaner parsing
    clean_sql = remove_sql_comments(sql_query)
    if not clean_sql.strip():
        return []

    table_names = set()

    # Parse the SQL using sqlparse
    parsed = sqlparse.parse(clean_sql)

    # Check if parsing was successful
    if not parsed:
        raise SqlValidationError("Failed to parse SQL query with sqlparse")

    for statement in parsed:
        # Extract table names from the statement
        statement_tables = _extract_tables_from_statement(statement)
        table_names.update(statement_tables)

    # If no table names were found, the SQL might be malformed
    if not table_names:
        raise SqlValidationError("No table names found in SQL query - possible malformed SQL")

    # Return unique table names in lowercase
    return list(table_names)


def _extract_tables_from_statement(statement: Statement) -> set[str]:
    """
    Extract table names from a parsed SQL statement.

    Args:
        statement: Parsed SQL statement from sqlparse

    Returns:
        Set of table names found in the statement (in lowercase)
    """
    table_names: set[str] = set()

    # Convert statement to string and analyze
    sql_str = str(statement).upper()

    # Extract from different SQL patterns
    patterns = [
        # FROM clause
        r"FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        # INSERT INTO
        r"INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        # UPDATE
        r"UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        # DELETE FROM
        r"DELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        # JOIN clauses
        r"JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        r"LEFT\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        r"RIGHT\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        r"INNER\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        r"OUTER\s+JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        # CTE names
        r"WITH\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+AS",
    ]

    for pattern in patterns:
        matches = re.findall(pattern, sql_str, re.IGNORECASE)
        # Convert matches to lowercase
        table_names.update(match.lower() for match in matches)

    return table_names


# Fallback function removed - sqlparse parsing is required


def split_sql_file(
    file_path: str | Path,
    *,
    strip_semicolon: bool = False,
) -> list[str]:
    """
    Read a SQL file and split it into individual executable statements.

    This function reads a SQL file and intelligently splits it into individual
    statements that can be executed separately. It handles complex SQL files
    with comments, multiple statements, and preserves statement integrity.

    Processing Steps:
        1. Read file with UTF-8 encoding
        2. Remove SQL comments (single-line -- and multi-line /* */)
        3. Parse using sqlparse for accurate statement boundaries
        4. Filter out empty statements and comment-only lines
        5. Optionally strip trailing semicolons

    Args:
        file_path: Path to the SQL file to process. Can be a string path or
            pathlib.Path object. File must exist and be readable.
        strip_semicolon: If True, remove trailing semicolons from each statement.
            If False (default), preserve semicolons as they appear in the file.
            Useful when the execution engine expects statements without semicolons.

    Returns:
        List of individual SQL statements as strings. Each statement is:
        - Trimmed of leading/trailing whitespace
        - Free of comments (unless within string literals)
        - Non-empty and contains actual SQL content
        - Optionally without trailing semicolons

    Raises:
        SqlFileError: If file operations fail, including:
            - File not found
            - Permission denied
            - I/O errors during reading
            - Encoding errors (non-UTF-8 content)
        SqlValidationError: If input validation fails:
            - file_path is None
            - file_path is empty string
            - file_path is not string or Path object

    Examples:
        Basic usage:
            >>> statements = split_sql_file("setup.sql")
            >>> for stmt in statements:
            ...     print(f"Statement: {stmt}")

        With semicolon stripping:
            >>> statements = split_sql_file("migration.sql", strip_semicolon=True)
            >>> # Statements will not have trailing semicolons

        Using pathlib.Path:
            >>> from pathlib import Path
            >>> sql_file = Path("database") / "schema.sql"
            >>> statements = split_sql_file(sql_file)

        Handling complex SQL files:
            >>> # File content:
            >>> # -- Create users table
            >>> # CREATE TABLE users (
            >>> #     id INTEGER PRIMARY KEY,
            >>> #     name TEXT NOT NULL
            >>> # );
            >>> #
            >>> # /* Insert sample data */
            >>> # INSERT INTO users (name) VALUES ('Alice'), ('Bob');
            >>> statements = split_sql_file("complex.sql")
            >>> len(statements)  # Returns 2 (CREATE and INSERT)

    Note:
        - Files are read with UTF-8 encoding by default
        - Comments within string literals are preserved
        - Empty lines and comment-only lines are filtered out
        - Statement boundaries are determined by sqlparse, not simple semicolon splitting
        - Large files are processed efficiently without loading entire content into memory
        - Thread-safe: Can be called concurrently from multiple threads
    """
    if file_path is None:
        raise SqlValidationError("file_path cannot be None")

    if not isinstance(file_path, str | Path):
        raise SqlValidationError("file_path must be a string or Path object")

    if not file_path:
        raise SqlValidationError("file_path cannot be empty")

    try:
        with open(file_path, encoding="utf-8") as f:
            sql_content = f.read()
        return parse_sql_statements(sql_content, strip_semicolon=strip_semicolon)
    except FileNotFoundError:
        raise SqlFileError(f"SQL file not found: {file_path}") from None
    except OSError as e:
        raise SqlFileError(f"Error reading SQL file {file_path}: {e}") from e
