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

SELECT
  TablesV.DatabaseName,
  TablesV.TableName,
  ColumnsV.ColumnName,
  ColumnsV.ColumnFormat,
  ColumnsV.ColumnTitle,
  ColumnsV.ColumnLength,
  ColumnsV.ColumnType,
  ColumnsV.DefaultValue,
  ColumnsV.ColumnConstraint,
  ColumnsV.ConstraintCount
FROM DBC.TablesV
INNER JOIN DBC.ColumnsV ON TablesV.DatabaseName = ColumnsV.DatabaseName
        AND TablesV.TableName = ColumnsV.TableName
WHERE
  TablesV.TableKind IN ('T', 'O', 'A', 'E', 'P', 'M', 'R', 'B', 'V') AND
  TablesV.DatabaseName NOT IN (
    'dbc', 'SYSJDBC', 'TD_SYSGPL', 'SYSLIB', 'SYSSPATIAL', 'TD_SYSXML',
    'Crashdumps', 'viewpoint', 'Sys_Calendar', 'EXTUSER', 'SYSUIF', 'TDStats',
    'LockLogShredder', 'External_AP', 'SysAdmin', 'dbcmngr', 'console',
    'TD_SYSFNLIB', 'SQLJ', 'TDQCD', 'TD_SERVER_DB', 'TDMaps', 'SystemFe',
    'TDPUSER', 'SYSUDTLIB', 'tdwm', 'SYSBAR');