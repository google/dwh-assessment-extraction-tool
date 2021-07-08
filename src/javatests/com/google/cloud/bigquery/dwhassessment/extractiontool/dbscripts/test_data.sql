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

INSERT INTO DBC.DiskSpaceV (
  VProc,
  DatabaseName,
  AccountName,
  MaxPerm,
  MaxSpool,
  MaxTemp,
  CurrentPerm,
  CurrentSpool,
  CurrentPersistentSpool,
  CurrentTemp,
  PeakPerm,
  PeakSpool,
  PeakPersistentSpool,
  PeakTemp,
  MaxProfileSpool,
  MaxProfileTemp,
  AllocatedPerm,
  AllocatedSpool,
  AllocatedTemp,
  PermSkew,
  SpoolSkew,
  TempSkew
)
VALUES (
  100,
  'db_name',
  'account_name',
  1100000,
  1200000,
  1300000,
  1400000,
  1500000,
  1600000,
  1700000,
  1800000,
  1900000,
  2000000,
  2100000,
  2200000,
  2300000,
  2400000,
  2500000,
  2600000,
  11,
  12,
  13
);

INSERT INTO DBC.QryLog (
  ProcID,
  CollectTimeStamp,
  QueryID,
  UserID,
  UserName,
  DefaultDatabase,
  AcctString,
  ExpandAcctString,
  SessionID,
  LogicalHostID,
  LogonDateTime,
  LogonSource,
  AppID,
  ClientID,
  ClientAddr,
  QueryText,
  StatementType,
  StatementGroup,
  StartTime,
  FirstRespTime,
  FirstStepTime,
  NumResultRows,
  AMPCPUTime,
  AMPCPUTimeNorm,
  NumOfActiveAmps,
  MaxStepMemory,
  TotalIOCount
)
VALUES (
  1,
  TIMESTAMP '2021-07-01 18:23:42',
  123,
  X'0A0B0C0D',
  'the_user',
  'default_db',
  'account',
  'expand account',
  9,
  2,
  TIMESTAMP '2021-07-01 18:05:06',
  'logon source',
  'app_id',
  'client_id',
  'client_address',
  'SELECT * FROM MyTable; SELECT * FROM YourTable;',
  'Select',
  'Select',
  TIMESTAMP '2021-07-01 18:15:06',
  TIMESTAMP '2021-07-01 18:15:08',
  TIMESTAMP '2021-07-01 18:15:09',
  65.0,
  1.23,
  0.123,
  2,
  123.45,
  1234.56
);

INSERT INTO DBC.TablesV (
  DatabaseName,
  TableName,
  AccessCount,
  LastAccessTimeStamp,
  LastAlterTimeStamp,
  TableKind,
  CreatorName,
  CreateTimeStamp,
  PrimaryKeyIndexId,
  ParentCount,
  ChildCount,
  CommitOpt
)
VALUES (
  'test_database',
  'test_table',
  100000000000,
  TIMESTAMP '2021-07-02 02:00:00',
  TIMESTAMP '2021-07-02 01:00:00',
  'V',
  'creator',
  TIMESTAMP '2021-07-02 00:00:00',
  10,
  2,
  10,
  'C'
), (
  'dbc',
  'test_table',
  100000000000,
  TIMESTAMP '2021-07-02 02:00:00',
  TIMESTAMP '2021-07-02 01:00:00',
  'V',
  'creator',
  TIMESTAMP '2021-07-02 00:00:00',
  10,
  2,
  10,
  'C'
);

INSERT INTO DBC.TableSizeV (
  DataBaseName,
  TableName,
  CurrentPerm,
  PeakPerm
)
VALUES (
  'test_database',
  'test_table',
  1000,
  2000
),(
  'dbc',
  'test_table',
  1000,
  2000
);

INSERT INTO DBC.ColumnsV (
  DatabaseName,
  TableName,
  ColumnName,
  ColumnFormat,
  ColumnTitle,
  ColumnLength,
  ColumnType,
  DefaultValue,
  ColumnConstraint,
  ConstraintCount
)
VALUES (
  'test_database',
  'test_table',
  'test_column',
  'test_format',
  'test_title',
  1000,
  'I',
  '0',
  'test constraint',
  1
), (
   'test_database',
   'not_existing_table',
   'test_column',
   'test_format',
   'test_title',
   1000,
   'I',
   '0',
   'test constraint',
   1
);