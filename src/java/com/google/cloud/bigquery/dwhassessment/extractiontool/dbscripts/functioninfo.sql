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
  DatabaseName AS "DatabaseName",
  FunctionName AS "FunctionName",
  SpecificName AS "SpecificName",
  FunctionId AS "FunctionId",
  NumParameters AS "NumParameters",
  ParameterDataTypes AS "ParameterDataTypes",
  FunctionType AS "FunctionType",
  ExternalName AS "ExternalName",
  SrcFileLanguage AS "SrcFileLanguage",
  NoSQLDataAccess AS "NoSQLDataAccess",
  ParameterStyle AS "ParameterStyle",
  DeterministicOpt AS "DeterministicOpt",
  NullCall AS "NullCall",
  PrepareCount AS "PrepareCount",
  ExecProtectionMode AS "ExecProtectionMode",
  ExtFileReference AS "ExtFileReference",
  CharacterType AS "CharacterType",
  Platform AS "Platform",
  InterimFldSize AS "InterimFldSize",
  RoutineKind AS "RoutineKind",
  ParameterUDTIds AS "ParameterUDTIds",
  MaxOutParameters AS "MaxOutParameters",
  GLOPSetDatabaseName AS "GLOPSetDatabaseName",
  GLOPSetMemberName AS "GLOPSetMemberName",
  RefQueryband AS "RefQueryband",
  ExecMapName AS "ExecMapName",
  ExecMapColocName AS "ExecMapColocName"
FROM DBC.FunctionsV;
