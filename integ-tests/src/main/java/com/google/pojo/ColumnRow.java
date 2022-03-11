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

/** POJO class for serialization data from DB and Avro files. */
@AutoValue
public abstract class ColumnRow {

  public abstract String dataBaseName();

  public abstract String tableName();

  public abstract String columnName();

  public abstract String columnFormat();

  public abstract String columnTitle();

  public abstract int columnLength();

  public abstract String columnType();

  public abstract String defaultValue();

  public abstract String columnConstraint();

  public abstract int constraintCount();

  public abstract String nullable();

  @Override
  public String toString() {
    return "{"
        + "dataBaseName="
        + dataBaseName()
        + ", tableName="
        + tableName()
        + ", columnName="
        + columnName()
        + ", columnFormat="
        + columnFormat()
        + ", columnTitle="
        + columnTitle()
        + ", columnLength="
        + columnLength()
        + ", columnType="
        + columnType()
        + ", defaultValue="
        + defaultValue()
        + ", columnConstraint="
        + columnConstraint()
        + ", constraintCount="
        + constraintCount()
        + ", nullable="
        + nullable()
        + "}"
        + lineSeparator();
  }

  public static ColumnRow create(
      String dataBaseName,
      String tableName,
      String columnName,
      String columnFormat,
      String columnTitle,
      int columnLength,
      String columnType,
      String defaultValue,
      String columnConstraint,
      int constraintCount,
      String nullable) {
    return new AutoValue_ColumnRow(
        dataBaseName,
        tableName,
        columnName,
        columnFormat,
        columnTitle,
        columnLength,
        columnType,
        defaultValue,
        columnConstraint,
        constraintCount,
        nullable);
  }
}
