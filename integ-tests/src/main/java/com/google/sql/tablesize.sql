SELECT
  "DatabaseName",
  "TableName",
  "CurrentPerm",
  "PeakPerm"
FROM "%s"."TableSizeV"
WHERE
  "DatabaseName" NOT IN (
    'dbc', 'SYSJDBC', 'TD_SYSGPL', 'SYSLIB', 'SYSSPATIAL', 'TD_SYSXML',
    'Crashdumps', 'viewpoint', 'Sys_Calendar', 'EXTUSER', 'SYSUIF',
    'TDStats', 'LockLogShredder', 'External_AP', 'SysAdmin', 'dbcmngr',
    'console', 'TD_SYSFNLIB', 'SQLJ', 'TDQCD', 'TD_SERVER_DB', 'TDMaps',
    'SystemFe', 'TDPUSER', 'SYSUDTLIB', 'tdwm', 'SYSBAR')