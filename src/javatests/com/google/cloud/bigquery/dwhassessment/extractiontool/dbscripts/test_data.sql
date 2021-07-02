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