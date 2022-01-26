package com.google.pojo;

import com.google.auto.value.AutoValue;

/**
 * POJO class for serialization data from DB and Avro files.
 */
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
        + "dataBaseName=" + dataBaseName()
        + ", tableName=" + tableName()
        + ", columnName=" + columnName()
        + ", columnFormat=" + columnFormat()
        + ", columnTitle=" + columnTitle()
        + ", columnLength=" + columnLength()
        + ", columnType=" + columnType()
        + ", defaultValue=" + defaultValue()
        + ", columnConstraint=" + columnConstraint()
        + ", constraintCount=" + constraintCount()
        + ", nullable=" + nullable()
        + "}\n";
  }

  public static ColumnRow create(String dataBaseName, String tableName, String columnName,
      String columnFormat, String columnTitle, int columnLength, String columnType,
      String defaultValue, String columnConstraint, int constraintCount, String nullable) {
    return new AutoValue_ColumnRow(dataBaseName, tableName, columnName, columnFormat, columnTitle,
        columnLength, columnType, defaultValue, columnConstraint, constraintCount, nullable);
  }
}
