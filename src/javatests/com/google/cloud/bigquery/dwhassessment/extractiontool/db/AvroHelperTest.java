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

import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.AvroHelper.dumpResults;
import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.AvroHelper.getAvroSchema;
import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.AvroHelper.getUnadjustedTimestamp;
import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.AvroHelper.parseRowToAvro;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class AvroHelperTest {

  private static Connection connection;
  private static ResultSetMetaData metaData;
  private final String TEST_SCHEMA =
      "{\"namespace\": \"namespace\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"schemaName\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"INT_COL\", \"type\": [\"null\", \"int\"], \"default\":null},\n"
          + "     {\"name\": \"VARCHAR_COL\", \"type\": [\"null\", \"string\"],"
          + " \"default\":null},\n"
          + "     {\"name\": \"CHAR_COL\", \"type\": [\"null\", \"string\"], \"default\":null},\n"
          + "     {\"name\": \"LONGVARCHAR_COL\", \"type\": [\"null\", \"string\"],"
          + " \"default\":null},\n"
          + "     {\"name\": \"SMALLINT_COL\", \"type\": [\"null\", \"int\"], \"default\":null},\n"
          + "     {\"name\": \"BIGINT_COL\", \"type\": [\"null\", \"long\"], \"default\":null},\n"
          + "     {\"name\": \"DECIMAL_COL\", \"type\": [\"null\","
          + " {\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":128,\"scale\":0}],"
          + " \"default\":null},\n"
          + "     {\"name\": \"TIMESTAMP_COL\", \"type\": [\"null\","
          + " {\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}], \"default\":null},\n"
          + "     {\"name\": \"DATE_COL\", \"type\": [\"null\","
          + " {\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}], \"default\":null},\n"
          + "     {\"name\": \"BINARY_COL\", \"type\": [\"null\", \"bytes\"], \"default\":null},\n"
          + "     {\"name\": \"FLOAT_COL\", \"type\": [\"null\", \"double\"], \"default\":null},\n"
          + "     {\"name\": \"DOUBLE_COL\", \"type\": [\"null\", \"double\"],"
          + " \"default\":null},\n"
          + "     {\"name\": \"BIT_COL\", \"type\": [\"null\", \"boolean\"], \"default\":null},\n"
          + "     {\"name\": \"BOOLEAN_COL\", \"type\": [\"null\", \"boolean\"],"
          + " \"default\":null},\n"
          + "     {\"name\": \"TINYINT_COL\", \"type\": [\"null\", \"int\"], \"default\":null},\n"
          + "     {\"name\": \"REAL_COL\", \"type\": [\"null\", \"double\"], \"default\":null}\n"
          + " ]\n"
          + "}";
  private final String SIMPLE_TEST_SCHEMA =
      "{\"namespace\": \"namespace\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"schemaName\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"ID\", \"type\": [\"null\", \"int\"], \"default\":null},\n"
          + "     {\"name\": \"NAME\", \"type\": [\"null\", \"string\"], \"default\":null},\n"
          + "     {\"name\": \"CHAR_COL\", \"type\": [\"null\", \"string\"], \"default\":null}\n"
          + " ]\n"
          + "}";

  @BeforeClass
  public static void setUp() throws Exception {
    connection = DriverManager.getConnection("jdbc:hsqldb:mem:db_1");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute(
        "CREATE TABLE T0 ("
            + "INT_COL INTEGER, "
            + "VARCHAR_COL VARCHAR(100), "
            + "CHAR_COL CHAR(100), "
            + "LONGVARCHAR_COL LONGVARCHAR(100), "
            + "SMALLINT_COL SMALLINT, "
            + "BIGINT_COL BIGINT, "
            + "DECIMAL_COL DECIMAL, "
            + "TIMESTAMP_COL TIMESTAMP, "
            + "DATE_COL DATE, "
            + "BINARY_COL BINARY, "
            + "FLOAT_COL FLOAT, "
            + "DOUBLE_COL DOUBLE, "
            + "BIT_COL BIT, "
            + "BOOLEAN_COL BOOLEAN, "
            + "TINYINT_COL TINYINT, "
            + "REAL_COL REAL)");
    baseStmt.execute(
        "CREATE TABLE SIMPLE_TABLE (ID INTEGER, NAME VARCHAR(100), CHAR_COL CHAR(20))");
    baseStmt.execute("INSERT INTO SIMPLE_TABLE VALUES (0, 'name_0', '  two  words')");
    baseStmt.close();
    connection.commit();

    metaData = connection.createStatement().executeQuery("SELECT * FROM T0").getMetaData();
  }

  @Test
  public void getAvroSchemaTest() throws Exception {
    Schema schema = getAvroSchema("schemaName", "namespace", metaData);
    assertThat(schema).isEqualTo(new Schema.Parser().parse(TEST_SCHEMA));
  }

  @Test
  public void getAvroSchema_emptySchemaName() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> getAvroSchema("", "namespace", metaData));
  }

  @Test
  public void getAvroSchema_emptyNamespace() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> getAvroSchema("schemaName", "", metaData));
  }

  @Test
  public void parseRowToAvroTest() throws Exception {
    Schema testSchema = new Schema.Parser().parse(SIMPLE_TEST_SCHEMA);
    ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM SIMPLE_TABLE");

    while (resultSet.next()) {
      GenericRecord result = parseRowToAvro(resultSet, testSchema);
      assertThat(result)
          .isEqualTo(
              new GenericRecordBuilder(testSchema)
                  .set("ID", 0)
                  .set("NAME", "name_0")
                  .set("CHAR_COL", "  two  words")
                  .build());
    }
  }

  @Test
  public void dumpResultsTest() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ScriptRunner scriptRunner = new ScriptRunnerImpl();
    ResultSetMetaData simpleTestMetadata =
        connection.createStatement().executeQuery("SELECT * FROM SIMPLE_TABLE").getMetaData();
    Schema schema = getAvroSchema("schemaName", "namespace", simpleTestMetadata);
    ImmutableList.Builder<GenericRecord> records = ImmutableList.builder();
    scriptRunner.executeScriptToAvro(
        connection, "SELECT * FROM SIMPLE_TABLE", schema, records::add);

    dumpResults(records.build(), outputStream, schema);

    DatumReader<Record> datumReader = new GenericDatumReader<>();
    DataFileReader<Record> reader =
        new DataFileReader<>(new SeekableByteArrayInput(outputStream.toByteArray()), datumReader);
    assertThat(reader.next())
        .isEqualTo(
            new GenericRecordBuilder(schema)
                .set("ID", 0)
                .set("NAME", "name_0")
                .set("CHAR_COL", "  two  words")
                .build());
  }

  @Test
  public void getUnadjustedTimestamp_byColumnIdFromTimestampWithTimeZone_correct()
      throws SQLException {
    // For Teradata JDBC, TIMESTAMP WITh TIME ZONE with value "2021-01-01 00:00:00.111-9:00" will be
    // returned as a timestamp "2021-01-01 00:00:00.111" and the input Calendar object's TimeZone
    // set to "GMT-9:00". That Timestamp object then has an incorrect value (depending on the system
    // time zone), whereas the correct value if represented in UTC time zone should be 2021-01-01
    // 09:00:00.111+0:00."
    Timestamp badAdjustedTimestamp = Timestamp.valueOf("2021-01-01 00:00:00.111");
    TimeZone teradataTimestampTimeZone = TimeZone.getTimeZone("GMT-9:00");
    Timestamp expectedUnadjustedTimestamp =
        Timestamp.from(Instant.parse("2021-01-01T09:00:00.111Z"));
    ResultSet mockTeradataResultSet = mock(ResultSet.class);
    doAnswer(
            invocation -> {
              Calendar cal = invocation.getArgument(1);
              cal.setTimeZone(teradataTimestampTimeZone);
              return badAdjustedTimestamp;
            })
        .when(mockTeradataResultSet)
        .getTimestamp(any(int.class), any(Calendar.class));

    Timestamp gotTimestamp = getUnadjustedTimestamp(mockTeradataResultSet, 1);

    assertThat(gotTimestamp).isEqualTo(expectedUnadjustedTimestamp);
  }

  @Test
  public void getUnadjustedTimestamp_byColumnNameFromTimestampWithTimeZone_correct()
      throws SQLException {
    // For Teradata JDBC, TIMESTAMP WITh TIME ZONE with value "2021-01-01 00:00:00.111-9:00" will be
    // returned as a timestamp "2021-01-01 00:00:00.111" and the input Calendar object's TimeZone
    // set to "GMT-9:00". That Timestamp object then has an incorrect value (depending on the system
    // time zone), whereas the correct value if represented in UTC time zone should be 2021-01-01
    // 09:00:00.111+0:00."
    Timestamp badAdjustedTimestamp = Timestamp.valueOf("2021-01-01 00:00:00.111");
    TimeZone teradataTimestampTimeZone = TimeZone.getTimeZone("GMT-9:00");
    Timestamp expectedUnadjustedTimestamp =
        Timestamp.from(Instant.parse("2021-01-01T09:00:00.111Z"));
    ResultSet mockTeradataResultSet = mock(ResultSet.class);
    doAnswer(
            invocation -> {
              Calendar cal = invocation.getArgument(1);
              cal.setTimeZone(teradataTimestampTimeZone);
              return badAdjustedTimestamp;
            })
        .when(mockTeradataResultSet)
        .getTimestamp(any(String.class), any(Calendar.class));

    Timestamp gotTimestamp = getUnadjustedTimestamp(mockTeradataResultSet, "column1");

    assertThat(gotTimestamp).isEqualTo(expectedUnadjustedTimestamp);
  }

  @Test
  public void getUnadjustedTimestamp_byColumnNameFromTimestampNoTimeZone_correct()
      throws SQLException {
    // For Teradata JDBC, TIMESTAMP without time zone info is returned as if it is the presentation
    // if invoked in the current time zone. We don't like this - thus we dictate the default time
    // zone to be UTC.
    Timestamp localTimestamp = Timestamp.valueOf("2021-01-01 00:00:00.111");
    Timestamp expectedUnadjustedTimestamp =
        Timestamp.from(Instant.parse("2021-01-01T00:00:00.111Z"));
    ResultSet mockTeradataResultSet = mock(ResultSet.class);
    doAnswer(
            invocation -> {
              Calendar cal = invocation.getArgument(1);
              TimeZone localTimeZone = cal.getTimeZone();
              return Timestamp.from(
                  ZonedDateTime.of(localTimestamp.toLocalDateTime(), localTimeZone.toZoneId())
                      .toInstant());
            })
        .when(mockTeradataResultSet)
        .getTimestamp(any(String.class), any(Calendar.class));

    Timestamp gotTimestamp = getUnadjustedTimestamp(mockTeradataResultSet, "column1");

    assertThat(gotTimestamp).isEqualTo(expectedUnadjustedTimestamp);
  }
}
