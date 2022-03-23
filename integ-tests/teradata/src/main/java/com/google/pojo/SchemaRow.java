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
public abstract class SchemaRow {

  public abstract String tableCat();

  public abstract String tableSchem();

  public abstract String tableName();

  public abstract String columnName();

  public abstract int dataType();

  public abstract String typeName();

  public abstract int columnSize();

  public abstract int bufferLength();

  public abstract int decimalDigits();

  public abstract int numPrecRadix();

  public abstract int nullable();

  public abstract String remarks();

  public abstract String columnDef();

  public abstract String sqlDataType();

  public abstract String sqlDatetimeSub();

  public abstract int charOctetLength();

  public abstract int ordinalPosition();

  public abstract String isNullable();

  public abstract String scopeCatlog();

  public abstract String scopeSchema();

  public abstract String scopeTable();

  public abstract int sourceDataType();

  @Override
  public String toString() {
    return "{"
        + "tableCat="
        + tableCat()
        + ", tableSchem="
        + tableSchem()
        + ", tableName="
        + tableName()
        + ", columnName="
        + columnName()
        + ", dataType="
        + dataType()
        + ", typeName="
        + typeName()
        + ", columnSize="
        + columnSize()
        + ", bufferLength="
        + bufferLength()
        + ", decimalDigits="
        + decimalDigits()
        + ", numPrecRadix="
        + numPrecRadix()
        + ", nullable="
        + nullable()
        + ", remarks="
        + remarks()
        + ", columnDef="
        + columnDef()
        + ", sqlDataType="
        + sqlDataType()
        + ", sqlDatetimeSub="
        + sqlDatetimeSub()
        + ", charOctetLength="
        + charOctetLength()
        + ", ordinalPosition="
        + ordinalPosition()
        + ", isNullable="
        + isNullable()
        + ", scopeCatlog="
        + scopeCatlog()
        + ", scopeSchema="
        + scopeSchema()
        + ", scopeTable="
        + scopeTable()
        + ", sourceDataType="
        + sourceDataType()
        + '}'
        + lineSeparator();
  }

  public static SchemaRow create(
      String tableCat,
      String tableSchem,
      String tableName,
      String columnName,
      int dataType,
      String typeName,
      int columnSize,
      int bufferLength,
      int decimalDigits,
      int numPrecRadix,
      int nullable,
      String remarks,
      String columnDef,
      String sqlDataType,
      String sqlDatetimeSub,
      int charOctetLength,
      int ordinalPosition,
      String isNullable,
      String scopeCatlog,
      String scopeSchema,
      String scopeTable,
      int sourceDataType) {
    return new AutoValue_SchemaRow(
        tableCat,
        tableSchem,
        tableName,
        columnName,
        dataType,
        typeName,
        columnSize,
        bufferLength,
        decimalDigits,
        numPrecRadix,
        nullable,
        remarks,
        columnDef,
        sqlDataType,
        sqlDatetimeSub,
        charOctetLength,
        ordinalPosition,
        isNullable,
        scopeCatlog,
        scopeSchema,
        scopeTable,
        sourceDataType);
  }
}
