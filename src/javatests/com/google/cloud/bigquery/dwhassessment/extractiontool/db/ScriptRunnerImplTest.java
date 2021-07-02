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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ScriptRunnerImplTest {

  private final String TEST_SCHEMA =
      "{\"namespace\": \"test.schema\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"test_name\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"ID\", \"type\": [\"int\", \"null\"]},\n"
          + "     {\"name\": \"NAME\", \"type\": [\"string\", \"null\"]}\n"
          + " ]\n"
          + "}";
  private ScriptRunner scriptRunner;

  @Before
  public void setUp() {
    scriptRunner = new ScriptRunnerImpl();
  }

  @Test
  public void executeScriptToAvro_providedSchemaSimpleScript() throws SQLException, IOException {
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:db_0");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute("CREATE TABLE T0 (ID INTEGER, NAME VARCHAR(100))");
    baseStmt.execute("INSERT INTO T0 VALUES (0, 'name_0')");
    baseStmt.close();
    connection.commit();

    Schema testSchema = new Schema.Parser().parse(TEST_SCHEMA);
    String sqlScript = "SELECT * FROM T0";
    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, sqlScript, testSchema);

    GenericRecord expectedRecord =
        new GenericRecordBuilder(testSchema).set("ID", 0).set("NAME", "name_0").build();
    assertThat(records).containsExactly(expectedRecord);
  }

  @Test
  public void executeScriptToAvro_extractSchemaSimpleScript() throws SQLException, IOException {
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:db_1");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute("CREATE TABLE T0 (ID INTEGER, NAME VARCHAR(100))");
    baseStmt.execute("INSERT INTO T0 VALUES (0, 'name_0')");
    baseStmt.close();
    connection.commit();

    String sqlScript = "SELECT * FROM T0";
    Schema testSchema = scriptRunner.extractSchema(connection, sqlScript, "testName", "namespace");
    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, sqlScript, testSchema);

    GenericRecord expectedRecord =
        new GenericRecordBuilder(testSchema).set("ID", 0).set("NAME", "name_0").build();
    assertThat(records).containsExactly(expectedRecord);
  }

  @Test
  public void executeScriptToAvro_nullValues_success() throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:test_db");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute("CREATE TABLE T1 (ID INTEGER, NAME VARCHAR(100))");
    baseStmt.execute("INSERT INTO T1 VALUES (0, 'name_0')");
    baseStmt.execute("INSERT INTO T1 VALUES (1, null)");
    baseStmt.execute("INSERT INTO T1 VALUES (null, 'name_2')");
    baseStmt.close();
    connection.commit();

    Schema testSchema = new Schema.Parser().parse(TEST_SCHEMA);
    String sqlScript = "SELECT * FROM T1";
    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, sqlScript, testSchema);

    GenericRecord expectedRecord0 =
        new GenericRecordBuilder(testSchema).set("ID", 0).set("NAME", "name_0").build();
    GenericRecord expectedRecord1 =
        new GenericRecordBuilder(testSchema).set("ID", 1).set("NAME", null).build();
    GenericRecord expectedRecord2 =
        new GenericRecordBuilder(testSchema).set("ID", null).set("NAME", "name_2").build();
    assertThat(records).containsExactly(expectedRecord0, expectedRecord1, expectedRecord2);
  }

  @Test
  public void executeScriptToAvro_DecimalColumn_success() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:test_db")) {
      try (Statement baseStmt = connection.createStatement()) {
        baseStmt.execute("CREATE TABLE DecimalColumnTable (ID INTEGER, VALUE DECIMAL(5,0))");
        baseStmt.execute("INSERT INTO DecimalColumnTable VALUES ((0, 100), (1, 200))");
      }
      connection.commit();

      Schema schema =
          SchemaBuilder.record("DecimalRecord")
              .fields()
              .name("ID")
              .type()
              .optional()
              .intType()
              .name("VALUE")
              .type()
              .optional()
              .type(LogicalTypes.decimal(5).addToSchema(Schema.create(Type.BYTES)))
              .endRecord();

      ImmutableList<GenericRecord> records =
          scriptRunner.executeScriptToAvro(
              connection, /*sqlScript=*/ "SELECT * FROM DecimalColumnTable", schema);

      assertThat(records)
          .containsExactly(
              new GenericRecordBuilder(schema)
                  .set("ID", 0)
                  .set("VALUE", ByteBuffer.wrap(BigInteger.valueOf(100).toByteArray()))
                  .build(),
              new GenericRecordBuilder(schema)
                  .set("ID", 1)
                  .set("VALUE", ByteBuffer.wrap(BigInteger.valueOf(200).toByteArray()))
                  .build());
    }
  }

  @Test
  public void executeScriptToAvro_ByteColumn_success() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:test_db")) {
      try (Statement baseStmt = connection.createStatement()) {
        baseStmt.execute("CREATE TABLE ByteColumnTable (ID INTEGER, VALUE BINARY(5))");
        baseStmt.execute("INSERT INTO ByteColumnTable VALUES (0, X'0A0B0C0D0E0F')");
      }
      connection.commit();

      Schema schema =
          SchemaBuilder.record("ByteRecord")
              .fields()
              .name("ID")
              .type()
              .optional()
              .intType()
              .name("VALUE")
              .type()
              .optional()
              .bytesType()
              .endRecord();

      ImmutableList<GenericRecord> records =
          scriptRunner.executeScriptToAvro(
              connection, /*sqlScript=*/ "SELECT * FROM ByteColumnTable", schema);

      assertThat(records)
          .containsExactly(
              new GenericRecordBuilder(schema)
                  .set("ID", 0)
                  .set("VALUE", ByteBuffer.wrap(new byte[] {10, 11, 12, 13, 14}))
                  .build());
    }
  }

  @Test
  public void executeScriptToAvro_TimestampColumn_success() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:test_db")) {
      try (Statement baseStmt = connection.createStatement()) {
        baseStmt.execute(
            "CREATE TABLE TimestampColumnTable ("
                + "ID INTEGER, "
                + "VALUE TIMESTAMP(6) WITH TIME ZONE)");
        baseStmt.execute(
            "INSERT INTO TimestampColumnTable VALUES (" //
                + "(0,  TIMESTAMP '2021-07-01 18:23:42' AT TIME ZONE INTERVAL '0:00' HOUR TO"
                + " MINUTE)," //
                + "(1, TIMESTAMP '2021-07-02 18:23:42' AT TIME ZONE INTERVAL '0:00' HOUR TO"
                + " MINUTE))");
      }
      connection.commit();

      Schema schema =
          SchemaBuilder.record("TimestampRecord")
              .fields()
              .name("ID")
              .type()
              .optional()
              .intType()
              .name("VALUE")
              .type()
              .optional()
              .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Type.LONG)))
              .endRecord();

      ImmutableList<GenericRecord> records =
          scriptRunner.executeScriptToAvro(
              connection, /*sqlScript=*/ "SELECT * FROM TimestampColumnTable", schema);

      assertThat(records)
          .containsExactly(
              new GenericRecordBuilder(schema)
                  .set("ID", 0)
                  .set("VALUE", Instant.parse("2021-07-01T18:23:42Z").toEpochMilli())
                  .build(),
              new GenericRecordBuilder(schema)
                  .set("ID", 1)
                  .set("VALUE", Instant.parse("2021-07-02T18:23:42Z").toEpochMilli())
                  .build());
    }
  }

  @Test
  public void executeScriptToAvro_FloatColumn_success() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:test_db")) {
      try (Statement baseStmt = connection.createStatement()) {
        baseStmt.execute("CREATE TABLE FloatColumnTable (ID INTEGER, VALUE FLOAT)");
        baseStmt.execute("INSERT INTO FloatColumnTable VALUES (0, 1.23)");
      }
      connection.commit();

      Schema schema =
          SchemaBuilder.record("FloatRecord")
              .fields()
              .name("ID")
              .type()
              .optional()
              .intType()
              .name("VALUE")
              .type()
              .optional()
              .doubleType()
              .endRecord();

      ImmutableList<GenericRecord> records =
          scriptRunner.executeScriptToAvro(
              connection, /*sqlScript=*/ "SELECT * FROM FloatColumnTable", schema);

      assertThat(records)
          .containsExactly(
              new GenericRecordBuilder(schema).set("ID", 0).set("VALUE", 1.23).build());
    }
  }
}
