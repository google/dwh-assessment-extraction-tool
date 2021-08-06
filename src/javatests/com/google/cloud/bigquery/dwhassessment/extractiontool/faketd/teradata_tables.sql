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

CREATE SCHEMA DBC;

CREATE TABLE DBC.TablesV (
  DatabaseName VARCHAR(128),
  TableName VARCHAR(128),
  AccessCount BIGINT,
  LastAccessTimeStamp TIMESTAMP(0),
  LastAlterTimeStamp TIMESTAMP(0),
  TableKind CHAR(1),
  CreatorName VARCHAR(128),
  CreateTimeStamp TIMESTAMP(0),
  PrimaryKeyIndexId SMALLINT,
  ParentCount SMALLINT,
  ChildCount SMALLINT,
  CommitOpt CHAR(1),
  PRIMARY KEY (DatabaseName, TableName)
);

CREATE TABLE DBC.TableSizeV (
  DatabaseName VARCHAR(128),
  TableName VARCHAR(128),
  CurrentPerm BIGINT,
  PeakPerm BIGINT,
  PRIMARY KEY (DatabaseName, TableName)
);

CREATE TABLE DBC.ColumnsV (
  DatabaseName VARCHAR(128),
  TableName VARCHAR(128),
  ColumnName VARCHAR(128),
  ColumnFormat VARCHAR(128),
  ColumnTitle VARCHAR(256),
  ColumnLength INTEGER,
  ColumnType CHAR(2),
  DefaultValue VARCHAR(1024),
  ColumnConstraint VARCHAR(8192),
  ConstraintCount SMALLINT,
  PRIMARY KEY (DatabaseName, TableName, ColumnName)
);

CREATE TABLE DBC.IndicesV (
  DatabaseName VARCHAR(128),
  TableName VARCHAR(128),
  IndexNumber SMALLINT,
  IndexType CHAR(1),
  IndexName VARCHAR(128),
  ColumnName VARCHAR(128),
  ColumnPosition SMALLINT,
  AccessCount BIGINT,
  UniqueFlag CHAR(1),
  PRIMARY KEY (DatabaseName, TableName)
);

CREATE TABLE DBC.UsersV (
  UserName VARCHAR(128),
  CreatorName VARCHAR(128),
  DefaultDatabase VARCHAR(128),
  CreateTimeStamp TIMESTAMP(0),
  RoleName VARCHAR(128),
  AccessCount BIGINT,
  PRIMARY KEY (UserName)
);

CREATE TABLE DBC.RoleMembersV (
  RoleName VARCHAR(128),
  Grantor VARCHAR(128),
  Grantee VARCHAR(128),
  WhenGranted TIMESTAMP(0),
  DefaultRole VARCHAR(1),
  WithAdmin CHAR(1)
);

CREATE TABLE DBC.DiskSpaceV (
  Vproc INT,
  DatabaseName VARCHAR(128),
  AccountName VARCHAR(128),
  MaxPerm BIGINT,
  MaxSpool BIGINT,
  MaxTemp BIGINT,
  CurrentPerm BIGINT,
  CurrentSpool BIGINT,
  CurrentPersistentSpool BIGINT,
  CurrentTemp BIGINT,
  PeakPerm BIGINT,
  PeakSpool BIGINT,
  PeakPersistentSpool BIGINT,
  PeakTemp BIGINT,
  MaxProfileSpool BIGINT,
  MaxProfileTemp BIGINT,
  AllocatedPerm BIGINT,
  AllocatedSpool BIGINT,
  AllocatedTemp BIGINT,
  PermSkew SMALLINT,
  SpoolSkew SMALLINT,
  TempSkew SMALLINT,
  PRIMARY KEY (DatabaseName)
);

CREATE TABLE DBC.QryLog (
  ProcID DECIMAL(5,0),
  CollectTimeStamp TIMESTAMP(6),
  QueryID DECIMAL(18,0),
  UserID BINARY(4),
  UserName CHAR(30),
  ProxyUser VARCHAR(128),
  ProxyRole VARCHAR(128),
  DefaultDatabase CHAR(30),
  AcctString CHAR(30),
  ExpandAcctString CHAR(30),
  SessionID INTEGER,
  LogicalHostID SMALLINT,
  LogonDateTime TIMESTAMP(0),
  LogonSource CHAR(128),
  AppID CHAR(30),
  ClientID CHAR(30),
  ClientAddr CHAR(45),
  QueryText VARCHAR(10000),
  StatementType CHAR(20),
  StatementGroup VARCHAR(128),
  StartTime TIMESTAMP(0),
  FirstRespTime TIMESTAMP(0),
  FirstStepTime TIMESTAMP(0),
  Statements INTEGER,
  NumResultRows FLOAT,
  AMPCPUTime FLOAT,
  AMPCPUTimeNorm FLOAT,
  NumOfActiveAmps INTEGER,
  MaxStepMemory FLOAT,
  ReqPhysIO FLOAT,
  TotalIOCount FLOAT,
  PRIMARY KEY (ProcID, CollectTimeStamp)
);

CREATE TABLE DBC.All_RI_ChildrenV (
  IndexId SMALLINT,
  IndexName VARCHAR(128),
  ChildDB VARCHAR(128),
  ChildTable VARCHAR(128),
  ChildKeyColumn VARCHAR(128),
  ParentDB VARCHAR(128),
  ParentTable VARCHAR(128),
  ParentKeyColumn VARCHAR(128),
  InconsistentFlag CHAR(1),
  CreatorName VARCHAR(128),
  CreateTimeStamp TIMESTAMP(0)
);

CREATE TABLE DBC.All_RI_ParentsV (
  IndexId SMALLINT,
  IndexName VARCHAR(128),
  ChildDB VARCHAR(128),
  ChildTable VARCHAR(128),
  ChildKeyColumn VARCHAR(128),
  ParentDB VARCHAR(128),
  ParentTable VARCHAR(128),
  ParentKeyColumn VARCHAR(128),
  InconsistentFlag CHAR(1),
  CreatorName VARCHAR(128),
  CreateTimeStamp TIMESTAMP(0)
);

CREATE TABLE DBC.FunctionsV (
  DatabaseName VARCHAR(128) NOT NULL,
  FunctionName VARCHAR(128) NOT NULL,
  SpecificName VARCHAR(128),
  FunctionId BINARY(6) NOT NULL,
  NumParameters SMALLINT NOT NULL,
  ParameterDataTypes VARCHAR(256),
  FunctionType VARCHAR(5),
  ExternalName CHAR(30) NOT NULL,
  SrcFileLanguage CHAR(1) NOT NULL,
  NoSQLDataAccess CHAR(1) NOT NULL,
  ParameterStyle CHAR(1) NOT NULL,
  DeterministicOpt CHAR(1) NOT NULL,
  NullCall CHAR(1) NOT NULL,
  PrepareCount CHAR(1),
  ExecProtectionMode CHAR(1) NOT NULL,
  ExtFileReference VARCHAR(1000),
  CharacterType SMALLINT NOT NULL,
  Platform CHAR(8) NOT NULL,
  InterimFldSize INTEGER NOT NULL,
  RoutineKind CHAR(1) NOT NULL,
  ParameterUDTIds VARBINARY(512),
  MaxOutParameters SMALLINT NOT NULL,
  GLOPSetDatabaseName VARCHAR(128),
  GLOPSetMemberName VARCHAR(128),
  RefQueryband CHAR(1),
  ExecMapName VARCHAR(128),
  ExecMapColocName VARCHAR(128),
  PRIMARY KEY (FunctionId)
);