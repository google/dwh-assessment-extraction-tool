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
  VProc AS "VProc",
  DatabaseName AS "DatabaseName",
  AccountName AS "AccountName",
  MaxPerm AS "MaxPerm",
  MaxSpool AS "MaxSpool",
  MaxTemp AS "MaxTemp",
  CurrentPerm AS "CurrentPerm",
  CurrentSpool AS "CurrentSpool",
  CurrentPersistentSpool AS "CurrentPersistentSpool",
  CurrentTemp AS "CurrentTemp",
  PeakPerm AS "PeakPerm",
  PeakSpool AS "PeakSpool",
  PeakPersistentSpool AS "PeakPersistentSpool",
  PeakTemp AS "PeakTemp",
  MaxProfileSpool AS "MaxProfileSpool",
  MaxProfileTemp AS "MaxProfileTemp",
  AllocatedPerm AS "AllocatedPerm",
  AllocatedSpool AS "AllocatedSpool",
  AllocatedTemp AS "AllocatedTemp",
  PermSkew AS "PermSkew",
  SpoolSkew AS "SpoolSkew",
  TempSkew AS "TempSkew"
FROM DBC.DiskSpaceV