SELECT
  "DatabaseName",
  "TableName",
  "AccessCount",
  "LastAccessTimeStamp" AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE "LastAccessTimeStamp",
  "LastAlterTimeStamp" AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE "LastAlterTimeStamp",
  "TableKind",
  "CreatorName",
  "CreateTimeStamp" AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE "CreateTimeStamp",
  "PrimaryKeyIndexId",
  "ParentCount",
  "ChildCount",
  "CommitOpt",
  "CheckOpt"
FROM "%s"."TablesV"
WHERE
  "TableKind" IN ('T', 'O', 'A', 'E', 'P', 'M', 'R', 'B', 'V') AND
  "DatabaseName" NOT IN (
    'dbc', 'SYSJDBC', 'TD_SYSGPL', 'SYSLIB', 'SYSSPATIAL', 'TD_SYSXML',
    'Crashdumps', 'viewpoint', 'Sys_Calendar', 'EXTUSER', 'SYSUIF', 'TDStats',
    'LockLogShredder', 'External_AP', 'SysAdmin', 'dbcmngr', 'console',
    'TD_SYSFNLIB', 'SQLJ', 'TDQCD', 'TD_SERVER_DB', 'TDMaps', 'SystemFe',
    'TDPUSER', 'SYSUDTLIB', 'tdwm', 'SYSBAR')