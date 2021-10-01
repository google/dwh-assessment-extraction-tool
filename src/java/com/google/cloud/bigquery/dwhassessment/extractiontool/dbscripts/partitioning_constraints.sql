SELECT
  DatabaseName AS "DatabaseName",
  IndexName AS "IndexName",
  IndexNumber AS "IndexNumber",
  ConstraintType AS "ConstraintType",
  ConstraintText AS "ConstraintText",
  ConstraintCollation AS "ConstraintCollation",
  CollationName AS "CollationName",
  CreatorName AS "CreatorName",
  CreateTimeStamp AS "CreateTimeStamp",
  CharSetID AS "CharSetID",
  SessionMode AS "SessionMode",
  ResolvedCurrent_Date AS "ResolvedCurrent_Date",
  ResolvedCurrent_TimeStamp AS "ResolvedCurrent_TimeStamp",
  DefinedCombinedPartitions AS "DefinedCombinedPartitions",
  MaxCombinedPartitions AS "MaxCombinedPartitions",
  PartitioningLevels AS "PartitioningLevels",
  ColumnPartitioningLevel AS "ColumnPartitioningLevel"
FROM DBC.PartitioningConstraintsV;