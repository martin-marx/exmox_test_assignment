#!/usr/bin/env bash
set -euo pipefail

# Requires the Databricks CLI to be authenticated and DATABRICKS_WAREHOUSE_ID.
volume_path="/Volumes/workspace/default/raw_data"

databricks sql execute --warehouse-id "$DATABRICKS_WAREHOUSE_ID" --file sql/ddl.sql

for entity in installs events offers user_profile; do
  databricks fs cp --overwrite "data/${entity}.csv" "${volume_path}/${entity}.csv"
done

printf 'Uploaded source files to %s\n' "$volume_path"
