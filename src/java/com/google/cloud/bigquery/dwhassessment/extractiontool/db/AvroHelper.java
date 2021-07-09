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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

/** A helper to convert sql result set to avro format and dump the avro result to output stream. */
public class AvroHelper {

  private AvroHelper() {}

  /**
   * Parse a row from sql result set to avro format in the form of a generic record.
   *
   * @param row A row of data from sql result set.
   * @param schema The avro schema object to build the avro generic record.
   * @return a generic record of the data in avro format.
   */
  public static GenericRecord parseRowToAvro(ResultSet row, Schema schema) throws SQLException {
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    ResultSetMetaData metaData = row.getMetaData();
    for (int columnIndex = 1; columnIndex <= row.getMetaData().getColumnCount(); columnIndex++) {
      recordBuilder.set(
          metaData.getColumnName(columnIndex), getRowObject(metaData, row, columnIndex));
    }
    return recordBuilder.build();
  }

  /**
   * Dump generic records to output stream.
   *
   * @param records A list of generic records to write to output stream.
   * @param outputStream An output stream to write the records to.
   * @param schema Schema definition of the data to write.
   */
  public static void dumpResults(
      ImmutableList<GenericRecord> records, OutputStream outputStream, Schema schema)
      throws IOException {
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    records.stream()
        .forEach(
            record -> {
              try {
                writer.write(record, encoder);
              } catch (IOException e) {
                throw new IllegalStateException(
                    String.format(
                        "Failed to encode query result %s to file with error message: %s",
                        record.toString(), e.getMessage()));
              }
            });
    outputStream.close();
    encoder.flush();
  }

  /**
   * Get the avro schema given the sql result set metadata.
   *
   * @param schemaName Schema name of the schema.
   * @param namespace Namespace of the scheme.
   * @param metaData The result set metadata to extract and convert the avro schema from.
   * @return The avro schema object.
   */
  public static Schema getAvroSchema(
      String schemaName, String namespace, ResultSetMetaData metaData) throws SQLException {
    Preconditions.checkArgument(!schemaName.isEmpty(), "Schema name cannot be empty.");
    Preconditions.checkArgument(!namespace.isEmpty(), "Namespace cannot be empty.");
    SchemaBuilder.FieldAssembler<Schema> schemaAssembler =
        SchemaBuilder.record(schemaName).namespace(namespace).fields();

    for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
      SchemaBuilder.FieldBuilder<Schema> fieldBuilder =
          schemaAssembler.name(metaData.getColumnName(columnIndex));
      AvroHelper.convertColumnTypeToAvroType(fieldBuilder, metaData, columnIndex);
    }
    return schemaAssembler.endRecord();
  }

  private static void convertColumnTypeToAvroType(
      SchemaBuilder.FieldBuilder<Schema> fieldBuilder, ResultSetMetaData metaData, int columnIndex)
      throws SQLException {
    switch (metaData.getColumnType(columnIndex)) {
      case Types.BOOLEAN:
      case Types.BIT:
        fieldBuilder.type().optional().booleanType();
        break;
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        fieldBuilder.type().optional().intType();
        break;
      case Types.BIGINT:
        fieldBuilder.type().optional().longType();
        break;
      case Types.DECIMAL:
        {
          Schema decimalType =
              LogicalTypes.decimal(
                      metaData.getPrecision(columnIndex), metaData.getScale(columnIndex))
                  .addToSchema(Schema.create(Schema.Type.BYTES));
          fieldBuilder.type().optional().type(decimalType);
          break;
        }
      case Types.FLOAT:
      case Types.REAL:
      case Types.DOUBLE:
        fieldBuilder.type().optional().doubleType();
        break;
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
        fieldBuilder.type().optional().stringType();
        break;
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        {
          Schema timestampType =
              LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
          fieldBuilder.type().optional().type(timestampType);
          break;
        }
      case Types.BINARY:
      case Types.VARBINARY:
        fieldBuilder.type().optional().bytesType();
        break;
      default:
        // TODO: support all other types specified in java.sql.Types.
        throw new UnsupportedOperationException(
            String.format("Type %s is not implemented yet.", metaData.getColumnType(columnIndex)));
    }
  }

  private static Object getRowObject(ResultSetMetaData metaData, ResultSet row, int columnIndex)
      throws SQLException {
    switch (metaData.getColumnType(columnIndex)) {
      case Types.DECIMAL:
        {
          BigDecimal bigDecimal = row.getBigDecimal(columnIndex);
          return bigDecimal == null
              ? null
              : ByteBuffer.wrap(bigDecimal.toBigInteger().toByteArray());
        }
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        {
          Timestamp timestamp = row.getTimestamp(columnIndex);
          return timestamp == null ? null : timestamp.toInstant().toEpochMilli();
        }
      case Types.BINARY:
      case Types.VARBINARY:
        {
          byte[] blob = row.getBytes(columnIndex);
          return blob == null ? null : ByteBuffer.wrap(blob);
        }
      case Types.FLOAT:
        return Float.valueOf(row.getFloat(columnIndex));
      default:
        return row.getObject(columnIndex);
    }
  }
}
