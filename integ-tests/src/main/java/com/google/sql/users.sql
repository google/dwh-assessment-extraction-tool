SELECT
  "DatabaseName" AS "UserName",
  "CreatorName",
  "CreateTimeStamp",
  "LastAccessTimeStamp"
FROM "{0}"."DatabasesV"
WHERE "DBKind"=''U''