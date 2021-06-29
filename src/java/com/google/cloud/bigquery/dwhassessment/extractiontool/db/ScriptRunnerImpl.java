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
package com.google.cloud.bigquery.dwhassessment.extractiontool.db;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;


/**
 * Implementation of ScriptRunner. Executes SQL script and converts query results to desired format,
 * i.e. Avro.
 */
public class ScriptRunnerImpl implements ScriptRunner {

  @Override
  public ImmutableList<GenericRecord> executeScriptToAvro(
      Connection connection, String sqlScript, Schema schema)
      throws SQLException {
    ImmutableList.Builder<GenericRecord> recordsBuilder = ImmutableList.builder();
    ResultSet resultSet = connection.createStatement().executeQuery(sqlScript);
    while (resultSet.next()) {
      recordsBuilder.add(parseRowToAvro(resultSet, schema));
    }
    return recordsBuilder.build();
  }

  @Override
  public Schema extractSchema(Connection connection, String sqlScript, String schemaName,
      String namespace)
      throws SQLException {
    ResultSetMetaData metaData = connection.createStatement().executeQuery(sqlScript).getMetaData();
    SchemaBuilder.FieldAssembler<Schema> schemaAssembler = SchemaBuilder.record(schemaName)
        .namespace(namespace).fields();

    for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
      SchemaBuilder.FieldBuilder<Schema> fieldBuilder = schemaAssembler
          .name(metaData.getColumnName(columnIndex));
      convertColumnTypeToAvroType(fieldBuilder, metaData.getColumnType(columnIndex));
    }
    return schemaAssembler.endRecord();
  }

  private GenericRecord parseRowToAvro(ResultSet row, Schema schema) throws SQLException {
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    ResultSetMetaData metaData = row.getMetaData();
    for (int columnIndex = 1; columnIndex <= row.getMetaData().getColumnCount(); columnIndex++) {
      recordBuilder.set(metaData.getColumnName(columnIndex), row.getObject(columnIndex));
    }
    return recordBuilder.build();
  }

  private static void convertColumnTypeToAvroType(
      SchemaBuilder.FieldBuilder<Schema> fieldBuilder, int columnType) {
    switch (columnType) {
      case java.sql.Types.CHAR:
      case java.sql.Types.VARCHAR:
      case java.sql.Types.LONGVARCHAR:
        fieldBuilder.type().optional().stringType();
        break;
      case java.sql.Types.INTEGER: {
        fieldBuilder.type().optional().intType();
        break;
        // TODO: support all other types specified in java.sql.Types.
      }
    }
  }
}
