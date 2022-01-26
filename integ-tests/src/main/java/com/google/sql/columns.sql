SELECT
  "TablesV"."DatabaseName",
  "TablesV"."TableName",
  "ColumnsV"."ColumnName",
  "ColumnsV"."ColumnFormat",
  "ColumnsV"."ColumnTitle",
  "ColumnsV"."ColumnLength",
  "ColumnsV"."ColumnType",
  "ColumnsV"."DefaultValue",
  "ColumnsV"."ColumnConstraint",
  "ColumnsV"."ConstraintCount",
  "ColumnsV"."Nullable",
  "ColumnsV"."UpperCaseFlag"
FROM "%s"."TablesV"
INNER JOIN "%s"."ColumnsV" ON "TablesV"."DatabaseName" = "ColumnsV"."DatabaseName"
        AND "TablesV"."TableName" = "ColumnsV"."TableName"
WHERE
  "TablesV"."TableKind" IN ('T', 'O', 'A', 'E', 'P', 'M', 'R', 'B', 'V') AND
  "TablesV"."DatabaseName" NOT IN (
    'dbc', 'SYSJDBC', 'TD_SYSGPL', 'SYSLIB', 'SYSSPATIAL', 'TD_SYSXML',
    'Crashdumps', 'viewpoint', 'Sys_Calendar', 'EXTUSER', 'SYSUIF', 'TDStats',
    'LockLogShredder', 'External_AP', 'SysAdmin', 'dbcmngr', 'console',
    'TD_SYSFNLIB', 'SQLJ', 'TDQCD', 'TD_SERVER_DB', 'TDMaps', 'SystemFe',
    'TDPUSER', 'SYSUDTLIB', 'tdwm', 'SYSBAR')