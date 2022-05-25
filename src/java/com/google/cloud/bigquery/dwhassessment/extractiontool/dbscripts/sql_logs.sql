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
  {{#if queryLogsVariables.needQueryText}}"{{#if vars.columnNameQueryText}}{{vars.columnNameQueryText}}{{else}}SqlTextInfo{{/if}}"{{else}}'_'{{/if}} AS "SqlText"
FROM {{#getTableName "QryLogSQLV"}}{{/getTableName}} AS "SQLV"
{{#whereClauseWithTimeRange queryLogsVariables "SQLV" "CollectTimeStamp"}}{{/whereClauseWithTimeRange}}
{{#if sortingColumns}}
ORDER BY {{#each sortingColumns}}"{{this}}"{{#unless @last}},{{/unless}}{{/each}} ASC NULLS FIRST
{{/if}}