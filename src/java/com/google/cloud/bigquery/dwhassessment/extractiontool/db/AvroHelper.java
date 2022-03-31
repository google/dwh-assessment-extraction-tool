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
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Pattern;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

/** A helper to convert sql result set to avro format and dump the avro result to output stream. */
public class AvroHelper {

  private static final Pattern TRAILING_SPACES_REGEX = Pattern.compile("\\s++$");

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
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
    dataFileWriter.create(schema, outputStream);

    records.stream()
        .forEach(
            record -> {
              try {
                dataFileWriter.append(record);
              } catch (IOException e) {
                throw new IllegalStateException(
                    String.format(
                        "Failed to encode query result %s to file with error message: %s",
                        record.toString(), e.getMessage()));
              }
            });
    dataFileWriter.close();
    outputStream.close();
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
      case Types.DATE:
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

  /**
   * Retrieves TIMESTAMP from target column in a ResultSet row, and associate it with proper time
   * zone information so that the resulted Timestamp correctly represents the logical equivalence of
   * Instant representation in Java.
   *
   * @param row One row as ResultSet.
   * @param columnName Name of the target column.
   * @return Unadjusted true timestamp.
   * @throws SQLException If JDBC fails to retrieve timestamp.
   */
  public static Timestamp getUnadjustedTimestamp(ResultSet row, String columnName)
      throws SQLException {
    // Note that if target column is TIMESTAMP WITH TIME ZONE, the TimeZone of cal will be set to
    // tht TIME ZONE value; if the target column is TIMESTAMP without time zone, the returned
    // Timestamp object is associated with the input
    // TimeZone of cal. We thus use default value "UTC" for TIMESTAMP columns, but let the TIMESTAMP
    // WITH TIME ZONE columns return their TIME ZONE via cal.
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    Timestamp timestamp = row.getTimestamp(columnName, cal);
    return unadjustTimestamp(timestamp, cal);
  }

  /**
   * Retrieves TIMESTAMP from target column in a ResultSet row, and associate it with proper time
   * zone information so that the resulted Timestamp correctly represents the logical equivalence of
   * Instant representation in Java.
   *
   * @param row One row as ResultSet.
   * @param columnIndex Index of the target column.
   * @return Unadjusted true timestamp.
   * @throws SQLException If JDBC fails to retrieve timestamp.
   */
  public static Timestamp getUnadjustedTimestamp(ResultSet row, int columnIndex)
      throws SQLException {
    // Note that if target column is TIMESTAMP WITH TIME ZONE, the TimeZone of cal will be set to
    // tht TIME ZONE value; if the target column is TIMESTAMP without time zone, the returned
    // Timestamp object is associated with the input
    // TimeZone of cal. We thus use default value "UTC" for TIMESTAMP columns, but let the TIMESTAMP
    // WITH TIME ZONE columns return their TIME ZONE via cal.
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    Timestamp timestamp = row.getTimestamp(columnIndex, cal);
    return unadjustTimestamp(timestamp, cal);
  }

  // Teradata's JDBC driver's ResultSet.getTimestamp() method disrespects both the TIMESTAMP WITH
  // TIME ZONE and the user's will with some twisted zone-adjustment logic (see Receiving DATE,
  // TIME, and TIMESTAMP Values from
  // https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html).
  // Unadjust it to make things right.
  private static Timestamp unadjustTimestamp(Timestamp timestamp, Calendar cal) {
    if (timestamp == null) {
      return null;
    }
    return Timestamp.from(
        ZonedDateTime.of(timestamp.toLocalDateTime(), cal.getTimeZone().toZoneId()).toInstant());
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
      case Types.DATE:
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        {
          Timestamp timestamp = getUnadjustedTimestamp(row, columnIndex);
          return timestamp == null ? null : timestamp.toInstant().toEpochMilli();
        }
      case Types.BINARY:
      case Types.VARBINARY:
        {
          byte[] blob = row.getBytes(columnIndex);
          return blob == null ? null : ByteBuffer.wrap(blob);
        }
      case Types.CHAR:
        {
          return trimTrailingSpaces(row.getString(columnIndex));
        }
      default:
        return row.getObject(columnIndex);
    }
  }

  private static String trimTrailingSpaces(String s) {
    return s == null ? null : TRAILING_SPACES_REGEX.matcher(s).replaceFirst("");
  }
}
