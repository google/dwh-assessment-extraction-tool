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
  AccessCount INTEGER,
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
  ColumnName VARCHAR(128),
  ColumnPosition SMALLINT,
  UniqueFlag CHAR(1),
  PRIMARY KEY (DatabaseName, TableName)
);

CREATE TABLE DBC.UsersV (
  UserName VARCHAR(128),
  CreatorName VARCHAR(128),
  DefaultDatabase VARCHAR(128),
  CreateTimeStamp TIMESTAMP(0),
  PRIMARY KEY (UserName)
);

CREATE TABLE DBC.DiskSpaceV (
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
  NumResultRows FLOAT,
  AMPCPUTime FLOAT,
  AMPCPUTimeNorm FLOAT,
  NumOfActiveAmps INTEGER,
  MaxStepMemory FLOAT,
  TotalIOCount FLOAT,
  PRIMARY KEY (ProcID, CollectTimeStamp)
);

CREATE TABLE DBC.All_RI_ChildrenV (
  ReferenceIdx SMALLINT,
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
  ReferenceIdx SMALLINT,
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
