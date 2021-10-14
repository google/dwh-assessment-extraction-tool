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

CREATE TABLE DBC."AllTempTablesVX" (
  "HostNo" SMALLINT,
  "SessionNo" INT,
  "UserName" VARCHAR(128),
  "B_DatabaseName" VARCHAR(128),
  "B_TableName" VARCHAR(128),
  "E_TableId" BINARY(6)
);

CREATE TABLE DBC."TablesV" (
  "DatabaseName" VARCHAR(128),
  "TableName" VARCHAR(128),
  "AccessCount" BIGINT,
  "LastAccessTimeStamp" TIMESTAMP(0),
  "LastAlterTimeStamp" TIMESTAMP(0),
  "TableKind" CHAR(1),
  "CreatorName" VARCHAR(128),
  "CreateTimeStamp" TIMESTAMP(0),
  "PrimaryKeyIndexId" SMALLINT,
  "ParentCount" SMALLINT,
  "ChildCount" SMALLINT,
  "CommitOpt" CHAR(1),
  "CheckOpt" CHAR(1),
  PRIMARY KEY ("DatabaseName", "TableName")
);

CREATE TABLE DBC."TableSizeV" (
  "DatabaseName" VARCHAR(128),
  "TableName" VARCHAR(128),
  "CurrentPerm" BIGINT,
  "PeakPerm" BIGINT,
  PRIMARY KEY ("DatabaseName", "TableName")
);

CREATE TABLE DBC."ColumnsV" (
  "DatabaseName" VARCHAR(128),
  "TableName" VARCHAR(128),
  "ColumnName" VARCHAR(128),
  "ColumnFormat" VARCHAR(128),
  "ColumnTitle" VARCHAR(256),
  "ColumnLength" INTEGER,
  "ColumnType" CHAR(2),
  "DefaultValue" VARCHAR(1024),
  "ColumnConstraint" VARCHAR(8192),
  "ConstraintCount" SMALLINT,
  "Nullable" CHAR(1),
  PRIMARY KEY ("DatabaseName", "TableName", "ColumnName")
);

CREATE TABLE DBC."IndicesV" (
  "DatabaseName" VARCHAR(128),
  "TableName" VARCHAR(128),
  "IndexNumber" SMALLINT,
  "IndexType" CHAR(1),
  "IndexName" VARCHAR(128),
  "ColumnName" VARCHAR(128),
  "ColumnPosition" SMALLINT,
  "AccessCount" BIGINT,
  "UniqueFlag" CHAR(1),
  PRIMARY KEY ("DatabaseName", "TableName")
);

CREATE TABLE DBC."DatabasesV" (
  "DatabaseName" VARCHAR(128),
  "CreatorName" VARCHAR(128),
  "OwnerName" VARCHAR(128),
  "AccountName" VARCHAR(128),
  "ProtectionType" VARCHAR(1),
  "JournalFlag" VARCHAR(2),
  "PermSpace" FLOAT,
  "SpoolSpace" FLOAT,
  "TempSpace" FLOAT,
  "CommentString" VARCHAR(255),
  "CreateTimeStamp" TIMESTAMP(0),
  "LastAlterName" VARCHAR(128),
  "LastAlterTimeStamp" TIMESTAMP(0),
  "DBKind" VARCHAR(1),
  "AccessCount" BIGINT,
  "LastAccessTimeStamp" TIMESTAMP(0)
);

CREATE TABLE DBC."RoleMembersV" (
  "RoleName" VARCHAR(128),
  "Grantor" VARCHAR(128),
  "Grantee" VARCHAR(128),
  "WhenGranted" TIMESTAMP(0),
  "DefaultRole" VARCHAR(1),
  "WithAdmin" CHAR(1)
);

CREATE TABLE DBC."DiskSpaceV" (
  "VProc" INT,
  "DatabaseName" VARCHAR(128),
  "AccountName" VARCHAR(128),
  "MaxPerm" BIGINT,
  "MaxSpool" BIGINT,
  "MaxTemp" BIGINT,
  "CurrentPerm" BIGINT,
  "CurrentSpool" BIGINT,
  "CurrentPersistentSpool" BIGINT,
  "CurrentTemp" BIGINT,
  "PeakPerm" BIGINT,
  "PeakSpool" BIGINT,
  "PeakPersistentSpool" BIGINT,
  "PeakTemp" BIGINT,
  "MaxProfileSpool" BIGINT,
  "MaxProfileTemp" BIGINT,
  "AllocatedPerm" BIGINT,
  "AllocatedSpool" BIGINT,
  "AllocatedTemp" BIGINT,
  "PermSkew" SMALLINT,
  "SpoolSkew" SMALLINT,
  "TempSkew" SMALLINT,
  PRIMARY KEY ("DatabaseName")
);

CREATE TABLE DBC."QryLogV" (
  "AbortFlag" CHAR(1),
  "AcctString" VARCHAR(128),
  "AcctStringDate" DATE,
  "AcctStringHour" SMALLINT,
  "AcctStringTime" FLOAT,
  "AMPCPUTime" FLOAT,
  "AMPCPUTimeNorm" FLOAT,
  "AppID" CHAR(30),
  "CacheFlag" CHAR(1),
  "CalendarName" VARCHAR(128),
  "CallNestingLevel" BINARY,
  "CheckpointNum" FLOAT,
  "ClientAddr" CHAR(45),
  "ClientID" CHAR(30),
  "CollectTimeStamp" TIMESTAMP(6),
  "CPUDecayLevel" SMALLINT,
  "DataCollectAlg" BINARY,
  "DBQLStatus" INTEGER,
  "DefaultDatabase" VARCHAR(128),
  "DelayTime" FLOAT,
  "DisCPUTime" FLOAT,
  "DisCPUTimeNorm" FLOAT,
  "ErrorCode" INTEGER,
  "EstMaxRowCount" FLOAT,
  "EstMaxStepTime" FLOAT,
  "EstProcTime" FLOAT,
  "EstResultRows" FLOAT,
  "ExpandAcctString" VARCHAR(128),
  "FirstRespTime" TIMESTAMP(6),
  "FirstStepTime" TIMESTAMP(6),
  "FlexThrottle" CHAR(1),
  "ImpactSpool" BIGINT,
  "InternalRequestNum" INTEGER,
  "IODecayLevel" SMALLINT,
  "IterationCount" INTEGER,
  "KeepFlag" CHAR(1),
  "LastRespTime" TIMESTAMP(6),
  "LockDelay" FLOAT,
  "LockLevel" VARCHAR(10),
  "LogicalHostID" SMALLINT,
  "LogonDateTime" TIMESTAMP(6),
  "LogonSource" CHAR(128),
  "LSN" FLOAT,
  "MaxAMPCPUTime" FLOAT,
  "MaxAMPCPUTimeNorm" FLOAT,
  "MaxAmpIO" INTEGER,
  "MaxCPUAmpNumber" INTEGER,
  "MaxCPUAmpNumberNorm" INTEGER,
  "MaxIOAmpNumber" INTEGER,
  "MaxNumMapAMPs" INTEGER,
  "MaxOneMBRowSize" INTEGER,
  "MaxStepMemory" FLOAT,
  "MaxStepsInPar" SMALLINT,
  "MinAmpCPUTime" FLOAT,
  "MinAmpCPUTimeNorm" FLOAT,
  "MinAmpIO" FLOAT,
  "MinNumMapAMPs" INTEGER,
  "MinRespHoldTime" FLOAT,
  "NumFragments" INTEGER,
  "NumOfActiveAMPs" INTEGER,
  "NumRequestCtx" BINARY,
  "NumResultOneMBRows" FLOAT,
  "NumResultRows" FLOAT,
  "NumSteps" SMALLINT,
  "NumStepswPar" SMALLINT,
  "ParamQuery" CHAR(1),
  "ParserCPUTime" FLOAT,
  "ParserCPUTimeNorm" FLOAT,
  "ParserExpReq" FLOAT,
  "PersistentSpool" BIGINT,
  "ProcID" DECIMAL(5,0),
  "ProfileID" BINARY,
  "ProfileName" VARCHAR(128),
  "ProxyRole" VARCHAR(128),
  "ProxyUser" VARCHAR(128),
  "ProxyUserID" BINARY,
  "QueryBand" VARCHAR(128),
  "QueryID" DECIMAL(18,0),
  "QueryRedriven" CHAR(1),
  "QueryText" VARCHAR(10000),
  "ReDriveKind" CHAR(10),
  "RemoteQuery" CHAR(1),
  "ReqIOKB"  FLOAT,
  "ReqPhysIO" FLOAT,
  "ReqPhysIOKB" FLOAT,
  "RequestMode" CHAR(5),
  "RequestNum"  INTEGER,
  "SeqRespTime" FLOAT,
  "SessionID" INTEGER,
  "SessionTemporalQualifier" VARCHAR(1024),
  "SpoolUsage" BIGINT,
  "StartTime" TIMESTAMP(6),
  "StatementGroup" VARCHAR(128),
  "Statements" INTEGER,
  "StatementType" CHAR(20) ,
  "SysDefNumMapAMPs" INTEGER,
  "TacticalCPUException" INTEGER,
  "TacticalIOException" INTEGER,
  "TDWMEstMemUsage" FLOAT,
  "ThrottleBypassed" CHAR(1),
  "TotalFirstRespTime" FLOAT,
  "TotalIOCount" FLOAT,
  "TotalServerByteCount" BIGINT,
  "TTGranularity" VARCHAR(30),
  "TxnMode" CHAR(10),
  "TxnUniq" BINARY,
  "UnitySQL" CHAR(1),
  "UnityTime" FLOAT,
  "UsedIota" FLOAT,
  "UserID" BINARY(4),
  "UserName" VARCHAR(128),
  "UtilityByteCount" BIGINT,
  "UtilityInfoAvailable" CHAR(1),
  "UtilityRowCount" FLOAT,
  "VHLogicalIO" FLOAT,
  "VHLogicalIOKB" FLOAT,
  "VHPhysIO" FLOAT,
  "VHPhysIOKB" FLOAT,
  "WarningOnly" CHAR(1),
  "WDName" VARCHAR(128)
);

CREATE TABLE DBC."All_RI_ChildrenV" (
  "IndexId" SMALLINT,
  "IndexName" VARCHAR(128),
  "ChildDB" VARCHAR(128),
  "ChildTable" VARCHAR(128),
  "ChildKeyColumn" VARCHAR(128),
  "ParentDB" VARCHAR(128),
  "ParentTable" VARCHAR(128),
  "ParentKeyColumn" VARCHAR(128),
  "InconsistencyFlag" CHAR(1),
  "CreatorName" VARCHAR(128),
  "CreateTimeStamp" TIMESTAMP(0)
);

CREATE TABLE DBC."All_RI_ParentsV" (
  "IndexId" SMALLINT,
  "IndexName" VARCHAR(128),
  "ChildDB" VARCHAR(128),
  "ChildTable" VARCHAR(128),
  "ChildKeyColumn" VARCHAR(128),
  "ParentDB" VARCHAR(128),
  "ParentTable" VARCHAR(128),
  "ParentKeyColumn" VARCHAR(128),
  "InconsistencyFlag" CHAR(1),
  "CreatorName" VARCHAR(128),
  "CreateTimeStamp" TIMESTAMP(0)
);

CREATE TABLE DBC."FunctionsV" (
  "DatabaseName" VARCHAR(128) NOT NULL,
  "FunctionName" VARCHAR(128) NOT NULL,
  "SpecificName" VARCHAR(128),
  "FunctionId" BINARY(6) NOT NULL,
  "NumParameters" SMALLINT NOT NULL,
  "ParameterDataTypes" VARCHAR(256),
  "FunctionType" VARCHAR(5),
  "ExternalName" CHAR(30) NOT NULL,
  "SrcFileLanguage" CHAR(1) NOT NULL,
  "NoSQLDataAccess" CHAR(1) NOT NULL,
  "ParameterStyle" CHAR(1) NOT NULL,
  "DeterministicOpt" CHAR(1) NOT NULL,
  "NullCall" CHAR(1) NOT NULL,
  "PrepareCount" CHAR(1),
  "ExecProtectionMode" CHAR(1) NOT NULL,
  "ExtFileReference" VARCHAR(1000),
  "CharacterType" SMALLINT NOT NULL,
  "Platform" CHAR(8) NOT NULL,
  "InterimFldSize" INTEGER NOT NULL,
  "RoutineKind" CHAR(1) NOT NULL,
  "ParameterUDTIds" VARBINARY(512),
  "MaxOutParameters" SMALLINT NOT NULL,
  "GLOPSetDatabaseName" VARCHAR(128),
  "GLOPSetMemberName" VARCHAR(128),
  "RefQueryband" CHAR(1),
  "ExecMapName" VARCHAR(128),
  "ExecMapColocName" VARCHAR(128),
  PRIMARY KEY ("FunctionId")
);

CREATE TABLE DBC."PartitioningConstraintsV" (
  "DatabaseName" VARCHAR(128) NOT NULL,
  "IndexName" VARCHAR(128),
  "IndexNumber" SMALLINT,
  "ConstraintType" CHAR(1) NOT NULL,
  "ConstraintText" VARCHAR(16000),
  "ConstraintCollation" CHAR(1) NOT NULL,
  "CollationName" VARCHAR(128),
  "CreatorName" VARCHAR(128) NOT NULL,
  "CreateTimeStamp" TIMESTAMP(0),
  "CharSetID" SMALLINT,
  "SessionMode" CHAR(1),
  "ResolvedCurrent_Date" DATE,
  "ResolvedCurrent_TimeStamp" TIMESTAMP(6) WITH TIME ZONE,
  "DefinedCombinedPartitions" BIGINT NOT NULL,
  "MaxCombinedPartitions" BIGINT NOT NULL,
  "PartitioningLevels" SMALLINT NOT NULL,
  "ColumnPartitioningLevel" SMALLINT NOT NULL,
);

CREATE TABLE DBC."DBQLObjTbl" (
  "ProcID" DECIMAL(5,0) NOT NULL,
  "CollectTimeStamp" TIMESTAMP(6) NOT NULL,
  "QueryID" DECIMAL(18,0) NOT NULL,
  "ObjectDatabaseName" VARCHAR(128),
  "ObjectTableName" VARCHAR(128),
  "ObjectColumnName" VARCHAR(128),
  "ObjectID" SMALLINT NOT NULL,
  "ObjectNum" INTEGER,
  "ObjectType" CHAR(3),
  "FreqofUse" INTEGER,
  "TypeofUse" SMALLINT
);

CREATE TABLE DBC."StatsV" (
  "DatabaseName" VARCHAR(128),
  "TableName" VARCHAR(128),
  "ColumnName" VARCHAR(128),
  "RowCount" INTEGER,
  "UniqueValueCount" INTEGER,
  "CreateTimeStamp" TIMESTAMP(6) NOT NULL
);