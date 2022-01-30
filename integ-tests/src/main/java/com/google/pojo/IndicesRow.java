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

import com.google.auto.value.AutoValue;

/**
 * POJO class for serialization data from DB and Avro files.
 */
@AutoValue
public abstract class IndicesRow {

  public abstract String dataBaseName();

  public abstract String tableName();

  public abstract int indexNumber();

  public abstract String indexType();

  public abstract String indexName();

  public abstract String columnName();

  public abstract int columnPosition();

  public abstract int accessCount();

  public abstract String uniqueFlag();

  @Override
  public String toString() {
    return "{"
        + "dataBaseName=" + dataBaseName()
        + ", tableName=" + tableName()
        + ", indexNumber=" + indexNumber()
        + ", indexType=" + indexType()
        + ", indexName=" + indexName()
        + ", columnName=" + columnName()
        + ", columnPosition=" + columnPosition()
        + ", accessCount=" + accessCount()
        + ", uniqueFlag=" + uniqueFlag()
        + "}\n";
  }

  public static IndicesRow create(String dataBaseName, String tableName, int indexNumber,
      String indexType, String indexName, String columnName, int columnPosition, int accessCount,
      String uniqueFlag) {
    return new AutoValue_IndicesRow(dataBaseName, tableName, indexNumber, indexType, indexName,
        columnName, columnPosition, accessCount, uniqueFlag);
  }
}
