SELECT
  "TablesV"."DatabaseName",
  "TablesV"."TableName",
  "IndicesV"."IndexNumber",
  "IndicesV"."IndexType",
  "IndicesV"."IndexName",
  "IndicesV"."ColumnName",
  "IndicesV"."ColumnPosition",
  "IndicesV"."AccessCount",
  "IndicesV"."UniqueFlag"
FROM "%s"."TablesV"
INNER JOIN "%s"."IndicesV" ON "TablesV"."DatabaseName" = "IndicesV"."DatabaseName"
        AND "TablesV"."TableName" = "IndicesV"."TableName"
WHERE
  "TablesV"."TableKind" IN ('T', 'O', 'A', 'E', 'P', 'M', 'R', 'B', 'V') AND
  "TablesV"."DatabaseName" NOT IN (
    'dbc', 'SYSJDBC', 'TD_SYSGPL', 'SYSLIB', 'SYSSPATIAL', 'TD_SYSXML',
    'Crashdumps', 'viewpoint', 'Sys_Calendar', 'EXTUSER', 'SYSUIF', 'TDStats',
    'LockLogShredder', 'External_AP', 'SysAdmin', 'dbcmngr', 'console',
    'TD_SYSFNLIB', 'SQLJ', 'TDQCD', 'TD_SERVER_DB', 'TDMaps', 'SystemFe',
    'TDPUSER', 'SYSUDTLIB', 'tdwm', 'SYSBAR')