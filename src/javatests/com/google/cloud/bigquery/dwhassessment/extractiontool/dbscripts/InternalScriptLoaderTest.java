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
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InternalScriptLoaderTest {

  private static final String DB_URL = "jdbc:hsqldb:mem:faketd;get_column_name=false";

  private static final Schema DECIMAL_5_TYPE =
      LogicalTypes.decimal(5).addToSchema(Schema.create(Type.BYTES));

  private static final Schema DECIMAL_18_TYPE =
      LogicalTypes.decimal(18).addToSchema(Schema.create(Type.BYTES));

  private static final Schema TIMESTAMP_MILLIS_TYPE =
      LogicalTypes.timestampMillis().addToSchema(Schema.create(Type.LONG));

  private static final String SCHEMA_NAMESPACE = "namespace";

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
  public void loadScripts_diskSpace() throws IOException, SQLException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(
        connection, "diskspace", new DataEntityManagerTesting(outputStream));

    SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("TimestampRecord").fields();
    fields = fields.name("VPROC").type().intType().noDefault();
    fields = fields.name("DATABASENAME").type().stringType().noDefault();
    fields = fields.name("ACCOUNTNAME").type().optional().stringType();
    fields = fields.name("MAXPERM").type().optional().longType();
    fields = fields.name("MAXSPOOL").type().optional().longType();
    fields = fields.name("MAXTEMP").type().optional().longType();
    fields = fields.name("CURRENTPERM").type().optional().longType();
    fields = fields.name("CURRENTSPOOL").type().optional().longType();
    fields = fields.name("CURRENTPERSISTENTSPOOL").type().optional().longType();
    fields = fields.name("CURRENTTEMP").type().optional().longType();
    fields = fields.name("PEAKPERM").type().optional().longType();
    fields = fields.name("PEAKSPOOL").type().optional().longType();
    fields = fields.name("PEAKPERSISTENTSPOOL").type().optional().longType();
    fields = fields.name("PEAKTEMP").type().optional().longType();
    fields = fields.name("MAXPROFILESPOOL").type().optional().longType();
    fields = fields.name("MAXPROFILETEMP").type().optional().longType();
    fields = fields.name("ALLOCATEDPERM").type().optional().longType();
    fields = fields.name("ALLOCATEDSPOOL").type().optional().longType();
    fields = fields.name("ALLOCATEDTEMP").type().optional().longType();
    fields = fields.name("PERMSKEW").type().optional().intType();
    fields = fields.name("SPOOLSKEW").type().optional().intType();
    fields = fields.name("TEMPSKEW").type().optional().intType();
    Schema schema = fields.endRecord();

    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(
            connection, /*sqlScript=*/ "SELECT * FROM DBC.DiskSpaceV", schema);

    assertThat(records)
        .containsExactly(
            new GenericRecordBuilder(schema)
                .set("VPROC", 100)
                .set("DATABASENAME", "db_name")
                .set("ACCOUNTNAME", "account_name")
                .set("MAXPERM", 1_100_000L)
                .set("MAXSPOOL", 1_200_000L)
                .set("MAXTEMP", 1_300_000L)
                .set("CURRENTPERM", 1_400_000L)
                .set("CURRENTSPOOL", 1_500_000L)
                .set("CURRENTPERSISTENTSPOOL", 1_600_000L)
                .set("CURRENTTEMP", 1_700_000L)
                .set("PEAKPERM", 1_800_000L)
                .set("PEAKSPOOL", 1_900_000L)
                .set("PEAKPERSISTENTSPOOL", 2_000_000L)
                .set("PEAKTEMP", 2_100_000L)
                .set("MAXPROFILESPOOL", 2_200_000L)
                .set("MAXPROFILETEMP", 2_300_000L)
                .set("ALLOCATEDPERM", 2_400_000L)
                .set("ALLOCATEDSPOOL", 2_500_000L)
                .set("ALLOCATEDTEMP", 2_600_000L)
                .set("PERMSKEW", 11)
                .set("SPOOLSKEW", 12)
                .set("TEMPSKEW", 13)
                .build());
  }

  @Test
  public void loadScripts_functioninfo() throws IOException, SQLException {
    String scriptName = "functioninfo";
    String sqlScript = scriptManager.getScript(scriptName);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));

    SchemaBuilder.FieldAssembler<Schema> fields =
        SchemaBuilder.record(scriptName).namespace(SCHEMA_NAMESPACE).fields();
    fields = fields.name("DATABASENAME").type().optional().stringType();
    fields = fields.name("FUNCTIONNAME").type().optional().stringType();
    fields = fields.name("SPECIFICNAME").type().optional().stringType();
    fields = fields.name("FUNCTIONID").type().optional().bytesType();
    fields = fields.name("NUMPARAMETERS").type().optional().intType();
    fields = fields.name("PARAMETERDATATYPES").type().optional().stringType();
    fields = fields.name("FUNCTIONTYPE").type().optional().stringType();
    fields = fields.name("EXTERNALNAME").type().optional().stringType();
    fields = fields.name("SRCFILELANGUAGE").type().optional().stringType();
    fields = fields.name("NOSQLDATAACCESS").type().optional().stringType();
    fields = fields.name("PARAMETERSTYLE").type().optional().stringType();
    fields = fields.name("DETERMINISTICOPT").type().optional().stringType();
    fields = fields.name("NULLCALL").type().optional().stringType();
    fields = fields.name("PREPARECOUNT").type().optional().stringType();
    fields = fields.name("EXECPROTECTIONMODE").type().optional().stringType();
    fields = fields.name("EXTFILEREFERENCE").type().optional().stringType();
    fields = fields.name("CHARACTERTYPE").type().optional().intType();
    fields = fields.name("PLATFORM").type().optional().stringType();
    fields = fields.name("INTERIMFLDSIZE").type().optional().intType();
    fields = fields.name("ROUTINEKIND").type().optional().stringType();
    fields = fields.name("PARAMETERUDTIDS").type().optional().bytesType();
    fields = fields.name("MAXOUTPARAMETERS").type().optional().intType();
    fields = fields.name("GLOPSETDATABASENAME").type().optional().stringType();
    fields = fields.name("GLOPSETMEMBERNAME").type().optional().stringType();
    fields = fields.name("REFQUERYBAND").type().optional().stringType();
    fields = fields.name("EXECMAPNAME").type().optional().stringType();
    fields = fields.name("EXECMAPCOLOCNAME").type().optional().stringType();
    Schema schema = fields.endRecord();

    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, sqlScript, schema);

    GenericRecord expectedRecord =
        new GenericRecordBuilder(schema)
            .set("DATABASENAME", "db_name")
            .set("FUNCTIONNAME", "function_name")
            .set("SPECIFICNAME", "specific_name")
            .set("FUNCTIONID", ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5, 6}))
            .set("NUMPARAMETERS", 3)
            .set("PARAMETERDATATYPES", "I1BF")
            .set("FUNCTIONTYPE", "F")
            .set("EXTERNALNAME", Strings.padEnd("JSONGETVALUE", 30, ' '))
            .set("SRCFILELANGUAGE", "P")
            .set("NOSQLDATAACCESS", "Y")
            .set("PARAMETERSTYLE", "I")
            .set("DETERMINISTICOPT", "Y")
            .set("NULLCALL", "Y")
            .set("PREPARECOUNT", "N")
            .set("EXECPROTECTIONMODE", "S")
            .set("EXTFILEREFERENCE", "SS!TD_GetFunctionContext!/var")
            .set("CHARACTERTYPE", 1)
            .set("PLATFORM", "LINUX64 ")
            .set("INTERIMFLDSIZE", 0)
            .set("ROUTINEKIND", "C")
            .set(
                "PARAMETERUDTIDS",
                ByteBuffer.wrap(
                    new byte[] {
                      0, 0, (byte) 0xEC, 0xC, 0, (byte) 0xC0, 0x30, 0, 0, (byte) 0xC0, 0x16, 0
                    }))
            .set("MAXOUTPARAMETERS", 0)
            .set("REFQUERYBAND", "N")
            .build();

    assertThat(records).containsExactly(expectedRecord);
    assertThat(getAvroDataOutputReader(outputStream).next()).isEqualTo(expectedRecord);
  }

  @Test
  public void loadScripts_indices() throws IOException, SQLException {
    String scriptName = "indices";
    String sqlScript = scriptManager.getScript(scriptName);
    // Get schema and verify records.
    Schema schema = scriptRunner.extractSchema(connection, sqlScript, scriptName, "namespace");
    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, /*sqlScript=*/ sqlScript, schema);
    GenericRecord expectedRecord =
        new GenericRecordBuilder(schema)
            .set("DATABASENAME", "test_database")
            .set("TABLENAME", "test_table")
            .set("INDEXNUMBER", 1)
            .set("INDEXTYPE", "P")
            .set("INDEXNAME", "index_name")
            .set("COLUMNNAME", "column_name")
            .set("COLUMNPOSITION", 2)
            .set("ACCESSCOUNT", 100000L)
            .set("UNIQUEFLAG", "U")
            .build();
    assertThat(records).containsExactly(expectedRecord);

    // Verify records serialization.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    assertThat(getAvroDataOutputReader(outputStream).next()).isEqualTo(expectedRecord);
  }

  @Test
  public void loadScripts_queryLogs() throws IOException, SQLException {
    String scriptName = "querylogs";

    SchemaBuilder.FieldAssembler<Schema> fields =
        SchemaBuilder.record(scriptName).namespace(SCHEMA_NAMESPACE).fields();
    fields = fields.name("PROCID").type().optional().type(DECIMAL_5_TYPE);
    fields = fields.name("COLLECTTIMESTAMP").type().optional().type(TIMESTAMP_MILLIS_TYPE);
    fields = fields.name("QUERYID").type().optional().type(DECIMAL_18_TYPE);
    fields = fields.name("USERID").type().optional().bytesType();
    fields = fields.name("USERNAME").type().optional().stringType();
    fields = fields.name("PROXYUSER").type().optional().stringType();
    fields = fields.name("PROXYROLE").type().optional().stringType();
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
    fields = fields.name("STATEMENTS").type().optional().intType();
    fields = fields.name("NUMRESULTROWS").type().optional().doubleType();
    fields = fields.name("AMPCPUTIME").type().optional().doubleType();
    fields = fields.name("AMPCPUTIMENORM").type().optional().doubleType();
    fields = fields.name("NUMOFACTIVEAMPS").type().optional().intType();
    fields = fields.name("MAXSTEPMEMORY").type().optional().doubleType();
    fields = fields.name("REQPHYSIO").type().optional().doubleType();
    fields = fields.name("TOTALFIRSTRESPTIME").type().optional().doubleType();
    fields = fields.name("TOTALIOCOUNT").type().optional().doubleType();
    Schema schema = fields.endRecord();

    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(
            connection, /*sqlScript=*/ "SELECT * FROM DBC.QryLog", schema);

    GenericRecord expectedRecord =
        new GenericRecordBuilder(schema)
            .set("PROCID", ByteBuffer.wrap(BigInteger.ONE.toByteArray()))
            .set("COLLECTTIMESTAMP", Instant.parse("2021-07-01T18:23:42Z").toEpochMilli())
            .set("QUERYID", ByteBuffer.wrap(BigInteger.valueOf(123).toByteArray()))
            .set("USERID", ByteBuffer.wrap(new byte[] {10, 11, 12, 13}))
            .set("USERNAME", Strings.padEnd("the_user", 30, ' '))
            .set("PROXYUSER", "proxy_user")
            .set("PROXYROLE", "proxy_role")
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
            .set("STATEMENTS", (int) 10)
            .set("NUMRESULTROWS", (double) 65.0)
            .set("AMPCPUTIME", (double) 1.23)
            .set("AMPCPUTIMENORM", (double) 0.123)
            .set("NUMOFACTIVEAMPS", 2)
            .set("MAXSTEPMEMORY", (double) 123.45)
            .set("REQPHYSIO", (double) 123.45)
            .set("TOTALFIRSTRESPTIME", (double) 1234.5)
            .set("TOTALIOCOUNT", (double) 1234.56)
            .build();

    assertThat(records).containsExactly(expectedRecord);

    // Verify records serialization.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    assertThat(getAvroDataOutputReader(outputStream).next()).isEqualTo(expectedRecord);
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
    assertThat(getAvroDataOutputReader(outputStream).next()).isEqualTo(expectedRecord);
  }

  @Test
  public void loadScripts_tableSize() throws IOException, SQLException {
    String scriptName = "tablesize";
    String sqlScript = scriptManager.getScript(scriptName);
    // Get schema and verify records.
    Schema schema = scriptRunner.extractSchema(connection, sqlScript, scriptName, "namespace");
    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, /*sqlScript=*/ sqlScript, schema);
    GenericRecord expectedRecord =
        new GenericRecordBuilder(schema)
            .set("DATABASENAME", "test_database")
            .set("TABLENAME", "test_table")
            .set("CURRENTPERM", 1000L)
            .set("PEAKPERM", 2000L)
            .build();
    assertThat(records).containsExactly(expectedRecord);

    // Verify records serialization.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    assertThat(getAvroDataOutputReader(outputStream).next()).isEqualTo(expectedRecord);
  }

  @Test
  public void loadScripts_columns() throws IOException, SQLException {
    String scriptName = "columns";
    String sqlScript = scriptManager.getScript(scriptName);
    // Get schema and verify records.
    Schema schema = scriptRunner.extractSchema(connection, sqlScript, scriptName, "namespace");
    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, /*sqlScript=*/ sqlScript, schema);
    GenericRecord expectedRecord =
        new GenericRecordBuilder(schema)
            .set("DATABASENAME", "test_database")
            .set("TABLENAME", "test_table")
            .set("COLUMNNAME", "test_column")
            .set("COLUMNFORMAT", "test_format")
            .set("COLUMNTITLE", "test_title")
            .set("COLUMNLENGTH", 1000)
            .set("COLUMNTYPE", "I ")
            .set("DEFAULTVALUE", "0")
            .set("COLUMNCONSTRAINT", "test constraint")
            .set("CONSTRAINTCOUNT", 1)
            .build();
    assertThat(records).containsExactly(expectedRecord);

    // Verify records serialization.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    assertThat(getAvroDataOutputReader(outputStream).next()).isEqualTo(expectedRecord);
  }

  @Test
  public void loadScripts_users() throws IOException, SQLException {
    String scriptName = "users";
    String sqlScript = scriptManager.getScript(scriptName);
    // Get schema and verify records.
    Schema schema = scriptRunner.extractSchema(connection, sqlScript, scriptName, "namespace");
    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, /*sqlScript=*/ sqlScript, schema);
    GenericRecord expectedRecordUser1 =
        new GenericRecordBuilder(schema)
            .set("USERNAME", "user_1")
            .set("CREATORNAME", "user_0")
            .set("CREATETIMESTAMP", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .build();
    GenericRecord expectedRecordUser2 =
        new GenericRecordBuilder(schema)
            .set("USERNAME", "user_2")
            .set("CREATORNAME", "user_0")
            .set("CREATETIMESTAMP", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .build();
    assertThat(records).containsExactly(expectedRecordUser1, expectedRecordUser2);

    // Verify records serialization.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    assertThat(readOutputStreamToAvro(outputStream, 2))
        .containsExactly(expectedRecordUser1, expectedRecordUser2);
  }

  @Test
  public void loadScripts_roles() throws Exception {
    String scriptName = "roles";
    String sqlScript = scriptManager.getScript(scriptName);
    // Get schema and verify records.
    Schema schema = scriptRunner.extractSchema(connection, sqlScript, scriptName, "namespace");
    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, /*sqlScript=*/ sqlScript, schema);
    GenericRecord expectedRecordRole1 =
        new GenericRecordBuilder(schema)
            .set("ROLENAME", "test_role_1")
            .set("GRANTOR", "user_0")
            .set("GRANTEE", "user_1")
            .set("WHENGRANTED", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .set("DEFAULTROLE", "Y")
            .set("WITHADMIN", "N")
            .build();
    GenericRecord expectedRecordRole2 =
        new GenericRecordBuilder(schema)
            .set("ROLENAME", "test_role_2")
            .set("GRANTOR", "user_0")
            .set("GRANTEE", "user_1")
            .set("WHENGRANTED", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .set("DEFAULTROLE", "N")
            .set("WITHADMIN", "Y")
            .build();
    GenericRecord expectedRecordRole3 =
        new GenericRecordBuilder(schema)
            .set("ROLENAME", "test_role_1")
            .set("GRANTOR", "user_0")
            .set("GRANTEE", "user_2")
            .set("WHENGRANTED", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .set("DEFAULTROLE", "N")
            .set("WITHADMIN", "N")
            .build();
    assertThat(records)
        .containsExactly(expectedRecordRole1, expectedRecordRole2, expectedRecordRole3);

    // Verify records serialization.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    assertThat(readOutputStreamToAvro(outputStream, 2))
        .containsExactly(expectedRecordRole1, expectedRecordRole2);
  }

  @Test
  public void loadScripts_all_ri_children() throws IOException, SQLException {
    String scriptName = "all_ri_children";
    String sqlScript = scriptManager.getScript(scriptName);
    // Get schema and verify records.
    Schema schema = scriptRunner.extractSchema(connection, sqlScript, scriptName, "namespace");
    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, /*sqlScript=*/ sqlScript, schema);
    GenericRecord expectedRecord1 =
        new GenericRecordBuilder(schema)
            .set("INDEXID", 0)
            .set("INDEXNAME", "index_name_0")
            .set("CHILDDB", "child_db_0")
            .set("CHILDTABLE", "child_table_0")
            .set("CHILDKEYCOLUMN", "child_key_column_0")
            .set("PARENTDB", "parent_db_0")
            .set("PARENTTABLE", "parent_table_0")
            .set("PARENTKEYCOLUMN", "parent_key_column_0")
            .set("INCONSISTENTFLAG", "Y")
            .set("CREATORNAME", "creator_0")
            .set("CREATETIMESTAMP", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .build();
    GenericRecord expectedRecord2 =
        new GenericRecordBuilder(schema)
            .set("INDEXID", 1)
            .set("INDEXNAME", "index_name_1")
            .set("CHILDDB", "child_db_1")
            .set("CHILDTABLE", "child_table_1")
            .set("CHILDKEYCOLUMN", "child_key_column_1")
            .set("PARENTDB", "parent_db_0")
            .set("PARENTTABLE", "parent_table_0")
            .set("PARENTKEYCOLUMN", "parent_key_column_0")
            .set("INCONSISTENTFLAG", "Y")
            .set("CREATORNAME", "creator_1")
            .set("CREATETIMESTAMP", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .build();
    assertThat(records).containsExactly(expectedRecord1, expectedRecord2);

    // Verify records serialization.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    assertThat(readOutputStreamToAvro(outputStream, 2))
        .containsExactly(expectedRecord1, expectedRecord2);
  }

  @Test
  public void loadScripts_all_ri_parents() throws IOException, SQLException {
    String scriptName = "all_ri_parents";
    String sqlScript = scriptManager.getScript(scriptName);
    // Get schema and verify records.
    Schema schema = scriptRunner.extractSchema(connection, sqlScript, scriptName, "namespace");
    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, /*sqlScript=*/ sqlScript, schema);
    GenericRecord expectedRecord1 =
        new GenericRecordBuilder(schema)
            .set("INDEXID", 0)
            .set("INDEXNAME", "index_name_0")
            .set("CHILDDB", "child_db_0")
            .set("CHILDTABLE", "child_table_0")
            .set("CHILDKEYCOLUMN", "child_key_column_0")
            .set("PARENTDB", "parent_db_0")
            .set("PARENTTABLE", "parent_table_0")
            .set("PARENTKEYCOLUMN", "parent_key_column_0")
            .set("INCONSISTENTFLAG", "Y")
            .set("CREATORNAME", "creator_0")
            .set("CREATETIMESTAMP", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .build();
    GenericRecord expectedRecord2 =
        new GenericRecordBuilder(schema)
            .set("INDEXID", 1)
            .set("INDEXNAME", "index_name_1")
            .set("CHILDDB", "child_db_1")
            .set("CHILDTABLE", "child_table_1")
            .set("CHILDKEYCOLUMN", "child_key_column_1")
            .set("PARENTDB", "parent_db_0")
            .set("PARENTTABLE", "parent_table_0")
            .set("PARENTKEYCOLUMN", "parent_key_column_0")
            .set("INCONSISTENTFLAG", "Y")
            .set("CREATORNAME", "creator_1")
            .set("CREATETIMESTAMP", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .build();
    assertThat(records).containsExactly(expectedRecord1, expectedRecord2);

    // Verify records serialization.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    assertThat(readOutputStreamToAvro(outputStream, 2))
        .containsExactly(expectedRecord1, expectedRecord2);
  }

  private ImmutableList<Record> readOutputStreamToAvro(
      ByteArrayOutputStream outputStream, int recordNum) throws IOException {
    DataFileReader<Record> reader = getAvroDataOutputReader(outputStream);
    ImmutableList.Builder<Record> result = ImmutableList.builder();
    for (int i = 0; i < recordNum; i++) {
      result.add(reader.next());
    }
    return result.build();
  }

  private DataFileReader<Record> getAvroDataOutputReader(ByteArrayOutputStream outputStream)
      throws IOException {
    DatumReader<Record> datumReader = new GenericDatumReader<>();
    return new DataFileReader<Record>(
        new SeekableByteArrayInput(outputStream.toByteArray()), datumReader);
  }
}
