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
public abstract class TablesizeRow {

  public abstract String dataBaseName();

  public abstract String tableName();

  public abstract long currentPerm();

  public abstract long peakPerm();

  @Override
  public String toString() {
    return "{"
        + "DataBaseName=" + dataBaseName()
        + ", TableName=" + tableName()
        + ", CurrentPerm=" + currentPerm()
        + ", PeakPerm=" + peakPerm()
        + "}" + lineSeparator();
  }

  public static TablesizeRow create(String dataBaseName, String tableName, long currentPerm,
      long peakPerm) {
    return new AutoValue_TablesizeRow(dataBaseName, tableName, currentPerm, peakPerm);
  }
}
