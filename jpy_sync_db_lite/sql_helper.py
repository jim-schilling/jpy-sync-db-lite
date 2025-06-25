"""
SQL Helper utilities for parsing and cleaning SQL statements.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""
from typing import List, Tuple


def remove_sql_comments(sql_text: str) -> str:
    """
    Remove SQL comments from a SQL string.
    
    Handles:
    - Single-line comments (-- comment)
    - Multi-line comments (/* comment */)
    - Preserves comments within string literals
    
    Args:
        sql_text: SQL string that may contain comments
        
    Returns:
        SQL string with comments removed
        
    Example:
        >>> sql = '''
        ... CREATE TABLE users (
        ...     id INTEGER PRIMARY KEY, -- user id
        ...     name TEXT NOT NULL,     /* user name */
        ...     email TEXT              -- user email
        ... );
        ... '''
        >>> clean_sql = remove_sql_comments(sql)
        >>> print(clean_sql)
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        );
    """
    if not sql_text:
        return sql_text
    
    # Handle string literals to avoid removing comments inside them
    result = []
    i = 0
    in_single_quote = False
    in_double_quote = False
    
    while i < len(sql_text):
        char = sql_text[i]
        
        # Handle string literals
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            result.append(char)
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            result.append(char)
        # Handle single-line comments (--)
        elif (char == '-' and i + 1 < len(sql_text) and sql_text[i + 1] == '-' 
              and not in_single_quote and not in_double_quote):
            # Skip until end of line
            while i < len(sql_text) and sql_text[i] != '\n':
                i += 1
            if i < len(sql_text):
                result.append('\n')  # Keep the newline
        # Handle multi-line comments (/* */)
        elif (char == '/' and i + 1 < len(sql_text) and sql_text[i + 1] == '*' 
              and not in_single_quote and not in_double_quote):
            # Skip until */
            i += 2  # Skip /*
            while i < len(sql_text) - 1:
                if sql_text[i] == '*' and sql_text[i + 1] == '/':
                    i += 2
                    break
                i += 1
        else:
            result.append(char)
        
        i += 1
    
    return ''.join(result)


def parse_sql_statements(sql_text: str, preserve_trailing_semicolon: bool = False) -> List[str]:
    """
    Parse a SQL string containing multiple statements into a list of individual statements.
    
    Handles:
    - Statements separated by semicolons
    - Preserves semicolons within string literals
    - Handles compound statements in BEGIN...END blocks (e.g., triggers)
    - Removes comments before parsing
    - Trims whitespace from individual statements
    - Filters out empty statements
    
    Args:
        sql_text: SQL string that may contain multiple statements
        preserve_trailing_semicolon: If True, preserve trailing semicolons in statements (default: False)
        
    Returns:
        List of individual SQL statements (with or without trailing semicolons based on parameter)
        
    Example:
        >>> sql = '''
        ... CREATE TABLE users (id INTEGER PRIMARY KEY);
        ... INSERT INTO users VALUES (1, 'John');
        ... SELECT * FROM users;
        ... '''
        >>> statements = parse_sql_statements(sql)
        >>> for stmt in statements:
        ...     print(f"Statement: {stmt}")
        Statement: CREATE TABLE users (id INTEGER PRIMARY KEY)
        Statement: INSERT INTO users VALUES (1, 'John')
        Statement: SELECT * FROM users
        
        >>> statements = parse_sql_statements(sql, preserve_trailing_semicolon=True)
        >>> for stmt in statements:
        ...     print(f"Statement: {stmt}")
        Statement: CREATE TABLE users (id INTEGER PRIMARY KEY);
        Statement: INSERT INTO users VALUES (1, 'John');
        Statement: SELECT * FROM users;
    """
    if not sql_text:
        return []
    
    # First remove comments
    clean_sql = remove_sql_comments(sql_text)
    
    # Split by semicolons, but be careful about semicolons in string literals and BEGIN...END blocks
    statements = []
    current_statement = []
    in_single_quote = False
    in_double_quote = False
    in_begin_block = False
    begin_count = 0
    i = 0
    
    while i < len(clean_sql):
        char = clean_sql[i]
        
        # Handle string literals
        if char == "'" and not in_double_quote and not in_begin_block:
            in_single_quote = not in_single_quote
            current_statement.append(char)
        elif char == '"' and not in_single_quote and not in_begin_block:
            in_double_quote = not in_double_quote
            current_statement.append(char)
        # Handle BEGIN...END blocks (triggers, not transactions)
        elif (char == 'B' and i + 4 < len(clean_sql) and 
              clean_sql[i:i+5].upper() == 'BEGIN' and 
              not in_single_quote and not in_double_quote):
            # Check if this is a trigger BEGIN (not BEGIN TRANSACTION)
            # Look ahead to see if this is followed by SQL statements, not TRANSACTION
            next_chars = clean_sql[i+5:i+20].strip().upper()
            if not next_chars.startswith('TRANSACTION'):
                in_begin_block = True
                begin_count += 1
            current_statement.append(char)
        elif (char == 'E' and i + 2 < len(clean_sql) and 
              clean_sql[i:i+3].upper() == 'END' and 
              not in_single_quote and not in_double_quote and in_begin_block):
            begin_count -= 1
            current_statement.append(char)
            if begin_count == 0:
                in_begin_block = False
        # Handle statement separators (semicolons) - but not inside BEGIN...END blocks
        elif char == ';' and not in_single_quote and not in_double_quote and not in_begin_block:
            # End of statement
            if preserve_trailing_semicolon:
                current_statement.append(char)  # Keep the semicolon
            statement = ''.join(current_statement).strip()
            if statement:  # Only add non-empty statements
                statements.append(statement)
            current_statement = []
        else:
            current_statement.append(char)
        
        i += 1
    
    # Handle the last statement (if no trailing semicolon)
    if current_statement:
        statement = ''.join(current_statement).strip()
        if statement:
            statements.append(statement)
    
    return statements


def split_sql_file(file_path: str, preserve_trailing_semicolon: bool = False) -> List[str]:
    """
    Read a SQL file and split it into individual statements.
    
    Args:
        file_path: Path to the SQL file
        preserve_trailing_semicolon: If True, preserve trailing semicolons in statements (default: False)
        
    Returns:
        List of individual SQL statements
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        IOError: If there's an error reading the file
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        return parse_sql_statements(sql_content, preserve_trailing_semicolon)
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    except IOError as e:
        raise IOError(f"Error reading SQL file {file_path}: {e}")


def validate_sql_statement(statement: str) -> Tuple[bool, str]:
    """
    Basic validation of a SQL statement.
    
    This is a simple validation that checks for common issues.
    For production use, consider using a proper SQL parser.
    
    Args:
        statement: SQL statement to validate
        
    Returns:
        Tuple of (is_valid, error_message)
        
    Example:
        >>> is_valid, error = validate_sql_statement("SELECT * FROM users")
        >>> print(is_valid, error)
        True ""
        
        >>> is_valid, error = validate_sql_statement("SELECT * FROM")
        >>> print(is_valid, error)
        False "Incomplete SELECT statement"
    """
    if not statement or not statement.strip():
        return False, "Empty statement"
    
    statement_stripped = statement.strip()
    statement_upper = statement_stripped.upper()
    
    # Check for basic SQL keywords
    if statement_upper.startswith('SELECT'):
        if 'FROM' not in statement_upper:
            return False, "SELECT statement missing FROM clause"
        # Check if there's content after FROM
        from_parts = statement_upper.split('FROM')
        if len(from_parts) < 2 or not from_parts[1].strip():
            return False, "SELECT statement missing table name after FROM"
    elif statement_upper.startswith('INSERT'):
        if 'INTO' not in statement_upper:
            return False, "INSERT statement missing INTO clause"
    elif statement_upper.startswith('UPDATE'):
        if 'SET' not in statement_upper:
            return False, "UPDATE statement missing SET clause"
    elif statement_upper.startswith('DELETE'):
        if 'FROM' not in statement_upper:
            return False, "DELETE statement missing FROM clause"
    elif statement_upper.startswith('CREATE'):
        if 'TABLE' not in statement_upper and 'INDEX' not in statement_upper and 'VIEW' not in statement_upper and 'TRIGGER' not in statement_upper:
            return False, "CREATE statement missing TABLE, INDEX, VIEW, or TRIGGER clause"
    elif statement_upper.startswith('DROP'):
        if 'TABLE' not in statement_upper and 'INDEX' not in statement_upper and 'VIEW' not in statement_upper and 'TRIGGER' not in statement_upper:
            return False, "DROP statement missing TABLE, INDEX, VIEW, or TRIGGER clause"
    elif statement_upper.startswith('ALTER'):
        if 'TABLE' not in statement_upper:
            return False, "ALTER statement missing TABLE clause"
    elif statement_upper.startswith('PRAGMA'):
        # PRAGMA must have an argument
        if len(statement_stripped.split()) < 2:
            return False, "PRAGMA statement missing argument"
    elif statement_upper.startswith('ATTACH'):
        # ATTACH DATABASE 'file' AS alias
        if 'DATABASE' not in statement_upper:
            return False, "ATTACH statement missing DATABASE keyword"
        # Check for AS keyword (must be a separate word)
        words = statement_upper.split()
        if 'AS' not in words:
            return False, "ATTACH statement missing AS keyword"
    elif statement_upper.startswith('DETACH'):
        # DETACH DATABASE alias
        if 'DATABASE' not in statement_upper:
            return False, "DETACH statement missing DATABASE keyword"
        # Check if there's a database name after DATABASE
        words = statement_upper.split()
        try:
            db_index = words.index('DATABASE')
            if db_index + 1 >= len(words):
                return False, "DETACH statement missing database name"
        except ValueError:
            return False, "DETACH statement missing DATABASE keyword"
    elif statement_upper.startswith('REINDEX'):
        # REINDEX [target]
        if len(statement_stripped.split()) < 2:
            return False, "REINDEX statement missing target"
    elif statement_upper.startswith('ANALYZE'):
        # ANALYZE [target]
        if len(statement_stripped.split()) < 2:
            return False, "ANALYZE statement missing target"
    elif statement_upper.startswith('VACUUM'):
        # VACUUM [schema-name] (optional, so always valid)
        return True, ""
    elif statement_upper.startswith('SAVEPOINT'):
        if len(statement_stripped.split()) < 2:
            return False, "SAVEPOINT statement missing name"
    elif statement_upper.startswith('RELEASE'):
        if len(statement_stripped.split()) < 2:
            return False, "RELEASE statement missing savepoint name"
    elif statement_upper.startswith('BEGIN'):
        # BEGIN [TRANSACTION] (optional)
        return True, ""
    elif statement_upper.startswith('COMMIT'):
        return True, ""
    elif statement_upper.startswith('ROLLBACK'):
        return True, ""
    elif statement_upper.startswith('EXPLAIN'):
        # EXPLAIN [QUERY PLAN] ...
        if len(statement_stripped.split()) < 2:
            return False, "EXPLAIN statement missing argument"
        return True, ""
    else:
        # Accept other statements for now
        return True, ""
    
    return True, ""


# Usage examples
if __name__ == "__main__":
    # Example 1: Remove comments
    sql_with_comments = """
    CREATE TABLE users (
        id INTEGER PRIMARY KEY, -- user id
        name TEXT NOT NULL,     /* user name */
        email TEXT              -- user email
    );
    """
    
    clean_sql = remove_sql_comments(sql_with_comments)
    print("Clean SQL:")
    print(clean_sql)
    print()
    
    # Example 2: Parse multiple statements (default behavior - strip semicolons)
    multi_sql = """
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
    INSERT INTO users VALUES (1, 'John');
    INSERT INTO users VALUES (2, 'Jane');
    SELECT * FROM users;
    """
    
    statements = parse_sql_statements(multi_sql)
    print("Individual statements (semicolons stripped):")
    for i, stmt in enumerate(statements, 1):
        print(f"{i}. {stmt}")
    print()
    
    # Example 3: Parse multiple statements (preserve semicolons)
    statements_with_semicolons = parse_sql_statements(multi_sql, preserve_trailing_semicolon=True)
    print("Individual statements (semicolons preserved):")
    for i, stmt in enumerate(statements_with_semicolons, 1):
        print(f"{i}. {stmt}")
    print()
    
    # Example 4: Validate statements
    test_statements = [
        "SELECT * FROM users",
        "INSERT INTO users VALUES (1, 'John')",
        "SELECT * FROM",  # Invalid
        "UPDATE users SET name = 'John' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
        "CREATE TABLE users (id INTEGER PRIMARY KEY)",
        "DROP TABLE users"
    ]
    
    print("Statement validation:")
    for stmt in test_statements:
        is_valid, error = validate_sql_statement(stmt)
        status = "✓" if is_valid else "✗"
        print(f"{status} {stmt}")
        if not is_valid:
            print(f"  Error: {error}") 