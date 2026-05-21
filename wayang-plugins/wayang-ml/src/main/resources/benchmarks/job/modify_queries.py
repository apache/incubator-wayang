import re
import os

# Set of SQL reserved keywords to avoid as aliases
RESERVED_KEYWORDS = {word.lower(): f"{word.lower()}_alias" for word in [
    "abort", "abs", "absolute", "access", "action", "add", "admin", "after", "aggregate", "alias", "all", "allocate",
    "alter", "analyse", "analyze", "and", "any", "are", "array", "as", "asc", "asensitive", "assertion", "associate",
    "asymmetric", "at", "atomic", "authorization", "avg", "backup", "before", "begin", "between", "bigint", "binary",
    "bit", "blob", "boolean", "both", "breadth", "by", "case", "cast", "char", "character", "check",
    "cluster", "column", "commit", "constraint", "convert", "copy", "count", "create", "cross",
    "current", "cursor", "cycle", "database", "date", "day", "decimal", "declare",
    "default", "delete", "desc", "distinct", "do", "drop", "each", "else", "end",
    "except", "execute", "exists", "explain", "false", "fetch", "filter", "float",
    "following", "for", "foreign", "from", "full", "function", "grant", "group",
    "having", "hour", "identity", "if", "ignore", "immediate", "in", "index",
    "inner", "insert", "int", "integer", "intersect", "interval", "into", "is",
    "join", "json", "key", "language", "last", "left", "level", "like", "limit",
    "local", "localtime", "localtimestamp", "lock", "long", "lower", "match", "max",
    "merge", "min", "minute", "mod", "mode", "modify", "month", "name", "natural",
    "new", "next", "no", "not", "null", "nullif", "number", "numeric", "offset",
    "old", "on", "only", "open", "option", "or", "order", "outer", "output", "over",
    "owner", "partition", "percent", "position", "power", "prepare", "primary",
    "procedure", "public", "range", "rank", "read", "real", "recursive",
    "references", "refresh", "release", "rename", "replace", "restrict",
    "return", "revoke", "right", "row", "rows", "savepoint", "schema",
    "search", "second", "select", "sensitive", "sequence", "session",
    "set", "show", "similar", "some", "specific", "sql", "sqlstate",
    "start", "state", "statistics", "substring", "sum", "symmetric",
    "system", "table", "temporary", "then", "time", "timestamp", "to",
    "transaction", "translate", "trigger", "true", "truncate", "union",
    "unique", "unknown", "update", "upper", "usage", "user", "using",
    "value", "values", "varchar", "view", "when", "where", "window",
    "with", "within", "work", "write", "xml", "year", "zone"
]}

def rename_reserved_aliases(query):
    """Finds and renames reserved SQL keywords used as aliases in the query."""
    alias_pattern = r'\bAS\s+(\w+)\b'

    def replace_alias(match):
        alias = match.group(1)
        if alias.lower() in RESERVED_KEYWORDS:
            return f"AS {RESERVED_KEYWORDS[alias.lower()]}"
        return match.group(0)

    return re.sub(alias_pattern, replace_alias, query, flags=re.IGNORECASE)

def replace_not_equal_operator(query):
    """Replaces every '!=' operator with '<>' to standardize SQL syntax."""
    return query.replace("!=", "<>")

def add_postgres_prefix_to_tables(query):
    """Prefixes 'postgres.' to all table names in the FROM clause, trims whitespace, and renames aliases."""
    from_clause_regex = r'FROM\s+([\w, \(\)\s]+)'

    match = re.search(from_clause_regex, query, re.IGNORECASE)
    if match:
        from_clause = match.group(1)
        tables = re.split(r'\s*,\s*', from_clause)
        prefixed_tables = [f"postgres.{table.strip()}" for table in tables]
        new_from_clause = 'FROM ' + ', '.join(prefixed_tables)
        query = query.replace(match.group(0), new_from_clause)

    query = rename_reserved_aliases(query)
    query = replace_not_equal_operator(query)
    query = re.sub(r'\s+', ' ', query).strip()

    return query

def process_queries(file_path, output_dir):
    """Reads queries from a file, modifies them, and saves each to its own file using line number as name."""
    with open(file_path, 'r') as file:
        queries = file.readlines()

    for line_number, query in enumerate(queries, start=1):
        sql_query = query.strip()
        if not sql_query:
            continue  # skip empty lines

        modified_query = add_postgres_prefix_to_tables(sql_query)

        output_file = f"{output_dir}/{line_number}.sql"
        with open(output_file, 'w') as f:
            f.write(modified_query + '\n')

        print(f"Processed and saved: {output_file}")

# Example usage
input_file = "./light/all.sql"  # Change this to your actual file
output_directory = "./light"  # Directory where queries will be saved

os.makedirs(output_directory, exist_ok=True)

process_queries(input_file, output_directory)

