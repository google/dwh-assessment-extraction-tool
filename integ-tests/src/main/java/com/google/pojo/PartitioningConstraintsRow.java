/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.pojo;

import static java.lang.System.lineSeparator;

import com.google.auto.value.AutoValue;

/**
 * POJO class for serialization data from DB and Avro files.
 */
@AutoValue
public abstract class PartitioningConstraintsRow {

  public abstract String databaseName();

  public abstract String tableName();

  public abstract String indexName();

  public abstract int indexNumber();

  public abstract String constraintType();

  public abstract String constraintText();

  public abstract String constraintCollation();

  public abstract String collationName();

  public abstract String creatorName();

  public abstract long createTimeStamp();

  public abstract String charSetID();

  public abstract String sessionMode();

  public abstract long resolvedCurrentDate();

  public abstract long resolvedCurrentTimeStamp();

  public abstract long definedCombinedPartitions();

  public abstract long maxCombinedPartitions();

  public abstract int partitioningLevels();

  public abstract int columnPartitioningLevel();

  @Override
  public String toString() {
    return "{"
        + "databaseName=" + databaseName()
        + ", tableName=" + tableName()
        + ", indexName=" + indexName()
        + ", indexNumber=" + indexNumber()
        + ", constraintType=" + constraintType()
        + ", constraintText=" + constraintText()
        + ", constraintCollation=" + constraintCollation()
        + ", collationName=" + collationName()
        + ", creatorName=" + creatorName()
        + ", createTimeStamp=" + createTimeStamp()
        + ", charSetID=" + charSetID()
        + ", sessionMode=" + sessionMode()
        + ", resolvedCurrent_Date=" + resolvedCurrentDate()
        + ", resolvedCurrent_TimeStamp=" + resolvedCurrentTimeStamp()
        + ", definedCombinedPartitions=" + definedCombinedPartitions()
        + ", maxCombinedPartitions=" + maxCombinedPartitions()
        + ", partitioningLevels=" + partitioningLevels()
        + ", columnPartitioningLevel=" + columnPartitioningLevel()
        + "}" + lineSeparator();
  }

  public static PartitioningConstraintsRow create(String databaseName, String tableName,
      String indexName, int indexNumber, String constraintType, String constraintText,
      String constraintCollation, String collationName, String creatorName, long createTimeStamp,
      String charSetID, String sessionMode, long resolvedCurrentDate,
      long resolvedCurrentTimeStamp,
      long definedCombinedPartitions, long maxCombinedPartitions, int partitioningLevels,
      int columnPartitioningLevel) {
    return new AutoValue_PartitioningConstraintsRow(databaseName, tableName, indexName, indexNumber,
        constraintType, constraintText, constraintCollation, collationName, creatorName,
        createTimeStamp, charSetID, sessionMode, resolvedCurrentDate, resolvedCurrentTimeStamp,
        definedCombinedPartitions, maxCombinedPartitions, partitioningLevels,
        columnPartitioningLevel);
  }
}
