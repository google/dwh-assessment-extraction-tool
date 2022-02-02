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
public abstract class AllRiChildrenRow {

  public abstract int indexID();
  public abstract String indexName();
  public abstract String childDB();
  public abstract String childTable();
  public abstract String childKeyColumn();
  public abstract String parentDB();
  public abstract String parentTable();
  public abstract String parentKeyColumn();
  public abstract String inconsistencyFlag();
  public abstract String creatorName();
  public abstract long createTimeStamp();

  @Override
  public String toString() {
    return "{"
        + "indexID=" + indexID()
        + ", indexName=" + indexName()
        + ", childDB=" + childDB()
        + ", childTable=" + childTable()
        + ", childKeyColumn=" + childKeyColumn()
        + ", parentDB=" + parentDB()
        + ", parentTable=" + parentTable()
        + ", parentKeyColumn=" + parentKeyColumn()
        + ", inconsistencyFlag=" + inconsistencyFlag()
        + ", creatorName=" + creatorName()
        + ", createTimeStamp=" + createTimeStamp()
        + "}\n";
  }

  public static AllRiChildrenRow create(int indexID, String indexName, String childDB,
      String childTable, String childKeyColumn, String parentDB, String parentTable,
      String parentKeyColumn, String inconsistencyFlag, String creatorName, long createTimeStamp) {
    return new AutoValue_AllRiChildrenRow(indexID, indexName, childDB, childTable, childKeyColumn,
        parentDB, parentTable, parentKeyColumn, inconsistencyFlag, creatorName, createTimeStamp);
  }
}
