/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License,
 *  Version 2.0 (the "License");
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

import com.google.auto.value.AutoValue;

/**
 * POJO class for serialization data from DB and Avro files.
 */
@AutoValue
public abstract class StatsRow {

  public abstract String databaseName();
  public abstract String tableName();
  public abstract String columnName();
  public abstract double rowCount();
  public abstract double uniqueValueCount();
  public abstract long createTimeStamp();

  @Override
  public String toString() {
    return "{"
        + "DatabaseName=" + databaseName()
        + ", TableName=" + tableName()
        + ", ColumnName=" + columnName()
        + ", RowCount=" + rowCount()
        + ", UniqueValueCount=" + uniqueValueCount()
        + ", CreateTimeStamp=" + createTimeStamp()
        + "}\n";
  }

  public static StatsRow create(String databaseName, String tableName, String columnName,
      double rowCount, double uniqueValueCount, long createTimeStamp) {
    return new AutoValue_StatsRow(databaseName, tableName, columnName, rowCount, uniqueValueCount,
        createTimeStamp);
  }
}
