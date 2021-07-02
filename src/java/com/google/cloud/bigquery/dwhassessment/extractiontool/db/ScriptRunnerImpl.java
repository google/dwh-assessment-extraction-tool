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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;


/**
 * Implementation of ScriptRunner. Executes SQL script and converts query results to desired format,
 * i.e. Avro.
 */
public class ScriptRunnerImpl implements ScriptRunner {

  private static void convertColumnTypeToAvroType(
      SchemaBuilder.FieldBuilder<Schema> fieldBuilder, ResultSetMetaData metaData,
      int columnIndex) throws SQLException {
    switch (metaData.getColumnType(columnIndex)) {
      case Types.CHAR:
      case Types.LONGVARCHAR:
      case Types.VARCHAR:
        fieldBuilder.type().optional().stringType();
        break;
      case Types.INTEGER:
      case Types.SMALLINT:
        fieldBuilder.type().optional().intType();
        break;
      case Types.BIGINT:
        fieldBuilder.type().optional().longType();
        break;
      case Types.DECIMAL: {
        Schema bigintType = LogicalTypes
            .decimal(metaData.getPrecision(columnIndex), metaData.getScale(columnIndex))
            .addToSchema(Schema.create(Type.BYTES));
        fieldBuilder.type().optional().type(bigintType);
        break;
      }
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE: {
        Schema timestampType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Type.LONG));
        fieldBuilder.type().optional().type(timestampType);
        break;
      }
      case Types.BINARY:
        fieldBuilder.type().optional().bytesType();
        break;
      case Types.DOUBLE:
        fieldBuilder.type().optional().doubleType();
        break;
      default:
        // TODO: support all other types specified in java.sql.Types.
        throw new UnsupportedOperationException(
            String.format("Type %s is not implemented yet.", metaData.getColumnType(columnIndex)));
    }
  }

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
      convertColumnTypeToAvroType(fieldBuilder, metaData, columnIndex);
    }
    return schemaAssembler.endRecord();
  }

  private GenericRecord parseRowToAvro(ResultSet row, Schema schema) throws SQLException {
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    ResultSetMetaData metaData = row.getMetaData();
    for (int columnIndex = 1; columnIndex <= row.getMetaData().getColumnCount(); columnIndex++) {
      recordBuilder
          .set(metaData.getColumnName(columnIndex), getRowObject(metaData, row, columnIndex));
    }
    return recordBuilder.build();
  }

  private Object getRowObject(ResultSetMetaData metaData, ResultSet row, int columnIndex)
      throws SQLException {
    switch (metaData.getColumnType(columnIndex)) {
      case Types.DECIMAL: {
        BigDecimal bigDecimal = row.getBigDecimal(columnIndex);
        return bigDecimal == null ? null : ByteBuffer.wrap(bigDecimal.toBigInteger().toByteArray());
      }
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE: {
        Timestamp timestamp = row.getTimestamp(columnIndex);
        return timestamp == null ? null : timestamp.toInstant().toEpochMilli();
      }
      default:
        return row.getObject(columnIndex);
    }
  }
}
