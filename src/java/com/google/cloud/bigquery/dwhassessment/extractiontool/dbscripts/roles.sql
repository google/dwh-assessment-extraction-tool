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

SELECT  RoleName AS "RoleName",
        Grantor AS "Grantor",
        Grantee AS "Grantee",
        WhenGranted AS "WhenGranted",
        DefaultRole AS "DefaultRole",
        WithAdmin AS "WithAdmin"
FROM DBC.RoleMembersV
WHERE   Grantee NOT IN ('TDPUSER', 'Crashdumps', 'tdwm', 'DBC',
        'LockLogShredder', 'TDMaps', 'Sys_Calendar', 'SysAdmin',
        'SystemFe', 'External_AP');