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
package com.google.cloud.bigquery.dwhassessment.extractiontool.dbscripts;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptManagerImpl;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptRunner;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptRunnerImpl;
import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManagerTesting;
import com.google.cloud.bigquery.dwhassessment.extractiontool.faketd.TeradataSimulator;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InternalScriptLoaderTest {

  private static final String DB_URL = "jdbc:hsqldb:mem:faketd";

  private static final Schema DECIMAL_5_TYPE =
      LogicalTypes.decimal(5).addToSchema(Schema.create(Type.BYTES));

  private static final Schema DECIMAL_18_TYPE =
      LogicalTypes.decimal(18).addToSchema(Schema.create(Type.BYTES));

  private static final Schema TIMESTAMP_MILLIS_TYPE =
      LogicalTypes.timestampMillis().addToSchema(Schema.create(Type.LONG));

  private static Connection connection;

  private final ScriptLoader scriptLoader = new InternalScriptLoader();
  private final ScriptManager scriptManager =
      new ScriptManagerImpl(new ScriptRunnerImpl(), scriptLoader.loadScripts());
  private final ScriptRunner scriptRunner = new ScriptRunnerImpl();

  @BeforeClass
  public static void setUpConnection() throws IOException, SQLException {
    TeradataSimulator.createTablesAndViews(DB_URL);
    TeradataSimulator.runSqlFile(
        DB_URL, InternalScriptLoaderTest.class.getResource("test_data.sql"));
    connection = DriverManager.getConnection(DB_URL);
  }

  @AfterClass
  public static void tearDownConnection() throws SQLException {
    connection.close();
  }

  @Test
  public void loadScripts_queryLogs() throws IOException, SQLException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(
        connection, "querylogs", new DataEntityManagerTesting(outputStream));

    SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("TimestampRecord").fields();
    fields = fields.name("PROCID").type().optional().type(DECIMAL_5_TYPE);
    fields = fields.name("COLLECTTIMESTAMP").type().optional().type(TIMESTAMP_MILLIS_TYPE);
    fields = fields.name("QUERYID").type().optional().type(DECIMAL_18_TYPE);
    fields = fields.name("USERID").type().optional().bytesType();
    fields = fields.name("USERNAME").type().optional().stringType();
    fields = fields.name("DEFAULTDATABASE").type().optional().stringType();
    fields = fields.name("ACCTSTRING").type().optional().stringType();
    fields = fields.name("EXPANDACCTSTRING").type().optional().stringType();
    fields = fields.name("SESSIONID").type().optional().intType();
    fields = fields.name("LOGICALHOSTID").type().optional().intType();
    fields = fields.name("LOGONDATETIME").type().optional().type(TIMESTAMP_MILLIS_TYPE);
    fields = fields.name("LOGONSOURCE").type().optional().stringType();
    fields = fields.name("APPID").type().optional().stringType();
    fields = fields.name("CLIENTID").type().optional().stringType();
    fields = fields.name("CLIENTADDR").type().optional().stringType();
    fields = fields.name("QUERYTEXT").type().optional().stringType();
    fields = fields.name("STATEMENTTYPE").type().optional().stringType();
    fields = fields.name("STATEMENTGROUP").type().optional().stringType();
    fields = fields.name("STARTTIME").type().optional().type(TIMESTAMP_MILLIS_TYPE);
    fields = fields.name("FIRSTRESPTIME").type().optional().type(TIMESTAMP_MILLIS_TYPE);
    fields = fields.name("FIRSTSTEPTIME").type().optional().type(TIMESTAMP_MILLIS_TYPE);
    fields = fields.name("NUMRESULTROWS").type().optional().doubleType();
    fields = fields.name("AMPCPUTIME").type().optional().doubleType();
    fields = fields.name("AMPCPUTIMENORM").type().optional().doubleType();
    fields = fields.name("NUMOFACTIVEAMPS").type().optional().intType();
    fields = fields.name("MAXSTEPMEMORY").type().optional().doubleType();
    fields = fields.name("TOTALIOCOUNT").type().optional().doubleType();
    Schema schema = fields.endRecord();

    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(
            connection, /*sqlScript=*/ "SELECT * FROM DBC.QryLog", schema);

    assertThat(records)
        .containsExactly(
            new GenericRecordBuilder(schema)
                .set("PROCID", ByteBuffer.wrap(BigInteger.ONE.toByteArray()))
                .set("COLLECTTIMESTAMP", Instant.parse("2021-07-01T18:23:42Z").toEpochMilli())
                .set("QUERYID", ByteBuffer.wrap(BigInteger.valueOf(123).toByteArray()))
                .set("USERID", ByteBuffer.wrap(new byte[] {10, 11, 12, 13}))
                .set("USERNAME", Strings.padEnd("the_user", 30, ' '))
                .set("DEFAULTDATABASE", Strings.padEnd("default_db", 30, ' '))
                .set("ACCTSTRING", Strings.padEnd("account", 30, ' '))
                .set("EXPANDACCTSTRING", Strings.padEnd("expand account", 30, ' '))
                .set("SESSIONID", 9)
                .set("LOGICALHOSTID", 2)
                .set("LOGONDATETIME", Instant.parse("2021-07-01T18:05:06Z").toEpochMilli())
                .set("LOGONSOURCE", Strings.padEnd("logon source", 128, ' '))
                .set("APPID", Strings.padEnd("app_id", 30, ' '))
                .set("CLIENTID", Strings.padEnd("client_id", 30, ' '))
                .set("CLIENTADDR", Strings.padEnd("client_address", 45, ' '))
                .set("QUERYTEXT", "SELECT * FROM MyTable; SELECT * FROM YourTable;")
                .set("STATEMENTTYPE", Strings.padEnd("Select", 20, ' '))
                .set("STATEMENTGROUP", "Select")
                .set("STARTTIME", Instant.parse("2021-07-01T18:15:06Z").toEpochMilli())
                .set("FIRSTRESPTIME", Instant.parse("2021-07-01T18:15:08Z").toEpochMilli())
                .set("FIRSTSTEPTIME", Instant.parse("2021-07-01T18:15:09Z").toEpochMilli())
                .set("NUMRESULTROWS", 65.0)
                .set("AMPCPUTIME", 1.23)
                .set("AMPCPUTIMENORM", 0.123)
                .set("NUMOFACTIVEAMPS", 2)
                .set("MAXSTEPMEMORY", 123.45)
                .set("TOTALIOCOUNT", 1234.56)
                .build());
  }

  @Test
  public void loadScripts_tableInfo() throws IOException, SQLException {
    String scriptName = "tableinfo";
    String sqlScript = scriptManager.getScript(scriptName);
    // Get schema and verify records.
    Schema schema = scriptRunner.extractSchema(connection, sqlScript, scriptName, "namespace");
    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, /*sqlScript=*/ sqlScript, schema);
    GenericRecord expectedRecord =
        new GenericRecordBuilder(schema)
            .set("DATABASENAME", "test_database")
            .set("TABLENAME", "test_table")
            .set("ACCESSCOUNT", 100_000_000_000L)
            .set("LASTACCESSTIMESTAMP", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .set("LASTALTERTIMESTAMP", Instant.parse("2021-07-02T01:00:00Z").toEpochMilli())
            .set("TABLEKIND", "V")
            .set("CREATORNAME", "creator")
            .set("CREATETIMESTAMP", Instant.parse("2021-07-02T00:00:00Z").toEpochMilli())
            .set("PRIMARYKEYINDEXID", 10)
            .set("PARENTCOUNT", 2)
            .set("CHILDCOUNT", 10)
            .set("COMMITOPT", "C")
            .build();
    assertThat(records).containsExactly(expectedRecord);

    // Verify records serialization.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    assertThat(readOutputStreamToAvro(outputStream, schema)).isEqualTo(expectedRecord);
  }

  private Record readOutputStreamToAvro(ByteArrayOutputStream outputStream, Schema schema)
      throws IOException {
    GenericDatumReader<Record> reader = new GenericDatumReader<>(schema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
    return reader.read(null, decoder);
  }
}
