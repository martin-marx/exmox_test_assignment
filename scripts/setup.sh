#!/usr/bin/env bash
set -euo pipefail

# Requires the Databricks CLI to be authenticated and DATABRICKS_WAREHOUSE_ID.
volume_path="/Volumes/test_assignment_marx/data/raw_data"

# The statements API runs one statement per call. Split sql/ddl.sql on ";" at the end of a line;
# semicolons inside COMMENT strings are followed by a space, so they are not split points.
jq -Rs -c --arg warehouse_id "$DATABRICKS_WAREHOUSE_ID" '
  split(";\n")[] | select(test("\\S"))
  | {warehouse_id: $warehouse_id, statement: ., wait_timeout: "50s"}
' sql/ddl.sql | while IFS= read -r payload; do
  response=$(databricks api post /api/2.0/sql/statements --json "$payload")
  if ! printf '%s' "$response" | jq -e '.status.state == "SUCCEEDED"' >/dev/null; then
    printf 'DDL statement failed:\n%s\n' "$response" >&2
    exit 1
  fi
done

for entity in installs events offers user_profile; do
  databricks fs cp --overwrite "data/${entity}.csv" "dbfs:${volume_path}/${entity}.csv"
done

printf 'Uploaded source files to %s\n' "$volume_path"
