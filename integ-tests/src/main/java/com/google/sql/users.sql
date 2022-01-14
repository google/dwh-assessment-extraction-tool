SELECT
  "DatabaseName" AS "UserName",
  "CreatorName",
  "CreateTimeStamp",
  "LastAccessTimeStamp"
FROM "%s"."DatabasesV"
WHERE "DBKind"='U'