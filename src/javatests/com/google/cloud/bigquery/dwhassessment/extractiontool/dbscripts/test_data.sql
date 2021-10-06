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

INSERT INTO DBC.AllTempTablesVX (
  HostNo,
  SessionNo,
  UserName,
  B_DatabaseName,
  B_TableName,
  E_TableId
)
VALUES(
  1,
  1,
  'user_name',
  'database_name',
  'table_name',
   X'010203040506'
);

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

INSERT INTO DBC.FunctionsV (
  DatabaseName,
  FunctionName,
  SpecificName,
  FunctionId,
  NumParameters,
  ParameterDataTypes,
  FunctionType,
  ExternalName,
  SrcFileLanguage,
  NoSQLDataAccess,
  ParameterStyle,
  DeterministicOpt,
  NullCall,
  PrepareCount,
  ExecProtectionMode,
  ExtFileReference,
  CharacterType,
  Platform,
  InterimFldSize,
  RoutineKind,
  ParameterUDTIds,
  MaxOutParameters,
  RefQueryband
) VALUES (
  'db_name',
  'function_name',
  'specific_name',
  X'010203040506',
  3,
  'I1BF',
  'F',
  'JSONGETVALUE',
  'P',
  'Y',
  'I',
  'Y',
  'Y',
  'N',
  'S',
  'SS!TD_GetFunctionContext!/var',
  1,
  'LINUX64',
  0,
  'C',
  X'0000EC0C00C0300000C01600',
  0,
  'N'
);

INSERT INTO DBC.IndicesV (
  DatabaseName,
  TableName,
  IndexNumber,
  IndexType,
  IndexName,
  ColumnName,
  ColumnPosition,
  AccessCount,
  UniqueFlag
)
VALUES (
  'test_database',
  'test_table',
  1,
  'P',
  'index_name',
  'column_name',
  2,
  100000,
  'U'
);

INSERT INTO DBC.QryLog (
  ProcID,
  CollectTimeStamp,
  QueryID,
  UserID,
  UserName,
  ProxyUser,
  ProxyRole,
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
  Statements,
  NumResultRows,
  AMPCPUTime,
  AMPCPUTimeNorm,
  NumOfActiveAmps,
  MaxStepMemory,
  ReqPhysIO,
  TotalFirstRespTime,
  TotalIOCount
)
VALUES (
  1,
  TIMESTAMP '2021-07-01 18:23:42',
  123,
  X'0A0B0C0D',
  'the_user',
  'proxy_user',
  'proxy_role',
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
  10,
  65.0,
  1.23,
  0.123,
  2,
  123.45,
  123.45,
  1234.5,
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
  ConstraintCount,
  Nullable
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
  1,
  'Y'
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
   1,
   'N'
);

INSERT INTO DBC.DatabasesV (
  DatabaseName,
  CreatorName,
  OwnerName,
  AccountName,
  ProtectionType,
  JournalFlag,
  PermSpace,
  SpoolSpace,
  TempSpace,
  CommentString,
  CreateTimeStamp,
  LastAlterName,
  LastAlterTimeStamp,
  DBKind,
  AccessCount,
  LastAccessTimeStamp
)
VALUES (
  'user_1',
  'user_0',
  'owner_name',
  'account_name',
  'F',
  'NN',
  100000,
  200000,
  20000000,
  null,
  TIMESTAMP '2021-07-02 02:00:00',
  'last_alter_name',
  TIMESTAMP '2021-07-01 02:00:00',
  'U',
  123,
  null
),(
    'user_2',
    'user_0',
    'owner_name_2',
    'account_name_2',
    'F',
    'NN',
    1000,
    2000,
    200000,
    null,
    TIMESTAMP '2021-07-02 02:00:00',
    'last_later_name_2',
    TIMESTAMP '2021-07-01 02:00:00',
    'U',
    45,
    TIMESTAMP '2021-07-03 02:00:00'
),(
    'non_user',
    'user_0',
    'owner_name',
    'account_name',
    'F',
    'NN',
    1000,
    2000,
    200000,
    null,
    TIMESTAMP '2021-07-02 02:00:00',
    'last_later_name',
    TIMESTAMP '2021-07-01 02:00:00',
    'N',
    145,
    null
  );

INSERT INTO DBC.RoleMembersV (
  RoleName,
  Grantee,
  Grantor,
  WhenGranted,
  DefaultRole,
  WithAdmin
)
VALUES (
  'test_role_1',
  'user_1',
  'user_0',
  TIMESTAMP '2021-07-02 02:00:00',
  'Y',
  'N'
),(
    'test_role_2',
    'user_1',
    'user_0',
    TIMESTAMP '2021-07-02 02:00:00',
    'N',
    'Y'
),(
    'test_role_1',
    'user_2',
    'user_0',
    TIMESTAMP '2021-07-02 02:00:00',
    'N',
    'N'
);

INSERT INTO DBC.All_RI_ChildrenV (
  IndexId,
  IndexName,
  ChildDB,
  ChildTable,
  ChildKeyColumn,
  ParentDB,
  ParentTable,
  ParentKeyColumn,
  InconsistencyFlag,
  CreatorName,
  CreateTimeStamp
)
VALUES (
  0,
  'index_name_0',
  'child_db_0',
  'child_table_0',
  'child_key_column_0',
  'parent_db_0',
  'parent_table_0',
  'parent_key_column_0',
  'Y',
  'creator_0',
  TIMESTAMP '2021-07-02 02:00:00'
), (
     1,
     'index_name_1',
     'child_db_1',
     'child_table_1',
     'child_key_column_1',
     'parent_db_0',
     'parent_table_0',
     'parent_key_column_0',
     'Y',
     'creator_1',
     TIMESTAMP '2021-07-02 02:00:00'
);

INSERT INTO DBC.All_RI_ParentsV (
  IndexId,
  IndexName,
  ChildDB,
  ChildTable,
  ChildKeyColumn,
  ParentDB,
  ParentTable,
  ParentKeyColumn,
  InconsistencyFlag,
  CreatorName,
  CreateTimeStamp
)
VALUES (
  0,
  'index_name_0',
  'child_db_0',
  'child_table_0',
  'child_key_column_0',
  'parent_db_0',
  'parent_table_0',
  'parent_key_column_0',
  'Y',
  'creator_0',
  TIMESTAMP '2021-07-02 02:00:00'
), (
     1,
     'index_name_1',
     'child_db_1',
     'child_table_1',
     'child_key_column_1',
     'parent_db_0',
     'parent_table_0',
     'parent_key_column_0',
     'Y',
     'creator_1',
     TIMESTAMP '2021-07-02 02:00:00'
);

INSERT INTO DBC.PartitioningConstraintsV (
  DatabaseName,
  IndexName,
  IndexNumber,
  ConstraintType,
  ConstraintText,
  ConstraintCollation,
  CollationName,
  CreatorName,
  CreateTimeStamp,
  CharSetID,
  DefinedCombinedPartitions,
  MaxCombinedPartitions,
  PartitioningLevels,
  ResolvedCurrent_Date,
  ColumnPartitioningLevel
)
VALUES (
  'db1',
  'index1',
  1,
  'K',
  'Foo',
  'A',
  'ASCII',
  'Creator',
  TIMESTAMP '2021-07-01 18:23:42',
  5,
  2916096,
  9223372036854775807,
  1,
  DATE '2021-07-01',
  0
);
