-- Copyright 2021 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- This SQL extracts all non-truncated SQL statements from DBC.QryLogSQLV by default
SELECT
  "ProcID",
  "CollectTimeStamp" AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS "CollectTimeStamp",
  "{{#if vars.columnNameRowNumber}}{{vars.columnNameRowNumber}}{{else}}SqlRowNo{{/if}}" AS "SqlRowNo",
  "{{#if vars.columnNameQueryID}}{{vars.columnNameQueryID}}{{else}}QueryID{{/if}}" AS "QueryID",
  "{{#if vars.columnNameQueryText}}{{vars.columnNameQueryText}}{{else}}SqlTextInfo{{/if}}" AS "SqlText"
FROM "{{baseDatabase}}"."{{#if vars.tableName}}{{vars.tableName}}{{else}}QryLogSQLV{{/if}}"
{{#if queryLogsVariables.timeRange}}
WHERE "{{baseDatabase}}"."{{#if vars.tableName}}{{vars.tableName}}{{else}}QryLogSQLV{{/if}}"."CollectTimeStamp"
  BETWEEN TIMESTAMP '{{queryLogsVariables.timeRange.startTimestamp}}' AND TIMESTAMP '{{queryLogsVariables.timeRange.endTimestamp}}'
{{/if}}