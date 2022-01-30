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
public abstract class TableinfoRow {

  public abstract String dataBaseName();
  public abstract String tableName();
  public abstract int accessCount();
  public abstract long lastAccessTimeStamp();
  public abstract long lastAlterTimeStamp();
  public abstract String tableKind();
  public abstract String creatorName();
  public abstract long createTimeStamp();
  public abstract int primaryKeyIndexId();
  public abstract int parentCount();
  public abstract String childCount();
  public abstract String commitOpt();

  @Override
  public String toString() {
    return "{"
        + "dataBaseName=" + dataBaseName()
        + ", tableName=" + tableName()
        + ", accessCount=" + accessCount()
        + ", lastAccessTimeStamp=" + lastAccessTimeStamp()
        + ", lastAlterTimeStamp=" + lastAlterTimeStamp()
        + ", tableKind=" + tableKind()
        + ", creatorName=" + creatorName()
        + ", createTimeStamp=" + createTimeStamp()
        + ", primaryKeyIndexId=" + primaryKeyIndexId()
        + ", parentCount=" + parentCount()
        + ", childCount=" + childCount()
        + ", commitOpt=" + commitOpt()
        + "}\n";
  }

  public static TableinfoRow create(String dataBaseName, String tableName, int accessCount,
      long lastAccessTimeStamp, long lastAlterTimeStamp, String tableKind, String creatorName,
      long createTimeStamp, int primaryKeyIndexId, int parentCount, String childCount,
      String commitOpt) {
    return new AutoValue_TableinfoRow(dataBaseName, tableName, accessCount, lastAccessTimeStamp,
        lastAlterTimeStamp, tableKind, creatorName, createTimeStamp, primaryKeyIndexId, parentCount,
        childCount, commitOpt);
  }


}
