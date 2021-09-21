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
  ProcID AS "ProcID",
  CollectTimeStamp AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS "CollectTimeStamp",
  QueryID AS "QueryID",
  UserID AS "UserID",
  UserName AS "UserName",
  ProxyUser AS "ProxyUser",
  ProxyRole AS "ProxyRole",
  DefaultDatabase AS "DefaultDatabase",
  AcctString AS "AcctString",
  ExpandAcctString AS "ExpandAcctString",
  SessionID AS "SessionID",
  LogicalHostID AS "LogicalHostID",
  LogonDateTime AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS "LogonDateTime",
  LogonSource AS "LogonSource",
  AppID AS "AppID",
  ClientID AS "ClientID",
  ClientAddr AS "ClientAddr",
  QueryText AS "QueryText",
  StatementType AS "StatementType",
  StatementGroup AS "StatementGroup",
  StartTime AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS "StartTime",
  FirstRespTime AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS "FirstRespTime",
  FirstStepTime AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS "FirstStepTime",
  Statements AS "Statements",
  NumResultRows AS "NumResultRows",
  AMPCPUTime AS "AMPCPUTime",
  AMPCPUTimeNorm AS "AMPCPUTimeNorm",
  NumOfActiveAmps AS "NumOfActiveAmps",
  MaxStepMemory AS "MaxStepMemory",
  ReqPhysIO AS "ReqPhysIO",
  TotalFirstRespTime AS "TotalFirstRespTime",
  TotalIOCount AS "TotalIOCount"
FROM DBC.QryLog