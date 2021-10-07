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
    String sqlScript = scriptManager.getScript("diskspace");

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(
        connection, "diskspace", new DataEntityManagerTesting(outputStream));

    SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("TimestampRecord").fields();
    fields = fields.name("VProc").type().intType().noDefault();
    fields = fields.name("DatabaseName").type().stringType().noDefault();
    fields = fields.name("AccountName").type().optional().stringType();
    fields = fields.name("MaxPerm").type().optional().longType();
    fields = fields.name("MaxSpool").type().optional().longType();
    fields = fields.name("MaxTemp").type().optional().longType();
    fields = fields.name("CurrentPerm").type().optional().longType();
    fields = fields.name("CurrentSpool").type().optional().longType();
    fields = fields.name("CurrentPersistentSpool").type().optional().longType();
    fields = fields.name("CurrentTemp").type().optional().longType();
    fields = fields.name("PeakPerm").type().optional().longType();
    fields = fields.name("PeakSpool").type().optional().longType();
    fields = fields.name("PeakPersistentSpool").type().optional().longType();
    fields = fields.name("PeakTemp").type().optional().longType();
    fields = fields.name("MaxProfileSpool").type().optional().longType();
    fields = fields.name("MaxProfileTemp").type().optional().longType();
    fields = fields.name("AllocatedPerm").type().optional().longType();
    fields = fields.name("AllocatedSpool").type().optional().longType();
    fields = fields.name("AllocatedTemp").type().optional().longType();
    fields = fields.name("PermSkew").type().optional().intType();
    fields = fields.name("SpoolSkew").type().optional().intType();
    fields = fields.name("TempSkew").type().optional().intType();
    Schema schema = fields.endRecord();

    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, sqlScript, schema);

    assertThat(records)
        .containsExactly(
            new GenericRecordBuilder(schema)
                .set("VProc", 100)
                .set("DatabaseName", "db_name")
                .set("AccountName", "account_name")
                .set("MaxPerm", 1_100_000L)
                .set("MaxSpool", 1_200_000L)
                .set("MaxTemp", 1_300_000L)
                .set("CurrentPerm", 1_400_000L)
                .set("CurrentSpool", 1_500_000L)
                .set("CurrentPersistentSpool", 1_600_000L)
                .set("CurrentTemp", 1_700_000L)
                .set("PeakPerm", 1_800_000L)
                .set("PeakSpool", 1_900_000L)
                .set("PeakPersistentSpool", 2_000_000L)
                .set("PeakTemp", 2_100_000L)
                .set("MaxProfileSpool", 2_200_000L)
                .set("MaxProfileTemp", 2_300_000L)
                .set("AllocatedPerm", 2_400_000L)
                .set("AllocatedSpool", 2_500_000L)
                .set("AllocatedTemp", 2_600_000L)
                .set("PermSkew", 11)
                .set("SpoolSkew", 12)
                .set("TempSkew", 13)
                .build());
  }

  @Test
  public void loadScripts_functioninfo() throws IOException, SQLException {
    String scriptName = "functioninfo";
    String sqlScript = scriptManager.getScript(scriptName);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    Schema schema = scriptRunner.extractSchema(connection, sqlScript, scriptName, "namespace");

    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, sqlScript, schema);

    GenericRecord expectedRecord =
        new GenericRecordBuilder(schema)
            .set("DatabaseName", "db_name")
            .set("FunctionName", "function_name")
            .set("SpecificName", "specific_name")
            .set("FunctionId", ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6}))
            .set("NumParameters", 3)
            .set("ParameterDataTypes", "I1BF")
            .set("FunctionType", "F")
            .set("ExternalName", Strings.padEnd("JSONGETVALUE", 30, ' '))
            .set("SrcFileLanguage", "P")
            .set("NoSQLDataAccess", "Y")
            .set("ParameterStyle", "I")
            .set("DeterministicOpt", "Y")
            .set("NullCall", "Y")
            .set("PrepareCount", "N")
            .set("ExecProtectionMode", "S")
            .set("ExtFileReference", "SS!TD_GetFunctionContext!/var")
            .set("CharacterType", 1)
            .set("Platform", "LINUX64 ")
            .set("InterimFldSize", 0)
            .set("RoutineKind", "C")
            .set(
                "ParameterUDTIds",
                ByteBuffer.wrap(
                    new byte[]{
                        0, 0, (byte) 0xEC, 0xC, 0, (byte) 0xC0, 0x30, 0, 0, (byte) 0xC0, 0x16, 0
                    }))
            .set("MaxOutParameters", 0)
            .set("RefQueryband", "N")
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
            .set("DatabaseName", "test_database")
            .set("TableName", "test_table")
            .set("IndexNumber", 1)
            .set("IndexType", "P")
            .set("IndexName", "index_name")
            .set("ColumnName", "column_name")
            .set("ColumnPosition", 2)
            .set("AccessCount", 100000L)
            .set("UniqueFlag", "U")
            .build();
    assertThat(records).containsExactly(expectedRecord);

    // Verify records serialization.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    assertThat(getAvroDataOutputReader(outputStream).next()).isEqualTo(expectedRecord);
  }

  @Test
  public void loadScripts_queryReferences() throws SQLException, IOException {
    String scriptName = "query_references";
    String sqlScript = scriptManager.getScript(scriptName);
    Schema schema = scriptRunner.extractSchema(connection, sqlScript, scriptName, "namespace");

    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, sqlScript, schema);

    GenericRecord expectedRecord =
        new GenericRecordBuilder(schema)
            .set("ProcID", ByteBuffer.wrap(BigInteger.ONE.toByteArray()))
            .set("CollectTimeStamp", Instant.parse("2021-07-01T18:23:42Z").toEpochMilli())
            .set("QueryID", ByteBuffer.wrap(BigInteger.valueOf(123).toByteArray()))
            .set("ObjectDatabaseName", "dbname")
            .set("ObjectTableName", "tablename")
            .set("ObjectColumnName", "columnname")
            .set("ObjectID", 5)
            .set("ObjectNum", 10)
            .set("ObjectType", "Col")
            .set("FreqofUse", 10)
            .set("TypeOfUse", 8)
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
    String sqlScript = scriptManager.getScript(scriptName);
    Schema schema = scriptRunner.extractSchema(connection, sqlScript, scriptName, "namespace");

    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, sqlScript, schema);

    GenericRecord expectedRecord =
        new GenericRecordBuilder(schema)
            .set("ProcID", ByteBuffer.wrap(BigInteger.ONE.toByteArray()))
            .set("CollectTimeStamp", Instant.parse("2021-07-01T18:23:42Z").toEpochMilli())
            .set("QueryID", ByteBuffer.wrap(BigInteger.valueOf(123).toByteArray()))
            .set("UserID", ByteBuffer.wrap(new byte[]{10, 11, 12, 13}))
            .set("UserName", Strings.padEnd("the_user", 30, ' '))
            .set("ProxyUser", "proxy_user")
            .set("ProxyRole", "proxy_role")
            .set("DefaultDatabase", Strings.padEnd("default_db", 30, ' '))
            .set("AcctString", Strings.padEnd("account", 30, ' '))
            .set("ExpandAcctString", Strings.padEnd("expand account", 30, ' '))
            .set("SessionID", 9)
            .set("LogicalHostID", 2)
            .set("LogonDateTime", Instant.parse("2021-07-01T18:05:06Z").toEpochMilli())
            .set("LogonSource", Strings.padEnd("logon source", 128, ' '))
            .set("AppID", Strings.padEnd("app_id", 30, ' '))
            .set("ClientID", Strings.padEnd("client_id", 30, ' '))
            .set("ClientAddr", Strings.padEnd("client_address", 45, ' '))
            .set("QueryText", "SELECT * FROM MyTable; SELECT * FROM YourTable;")
            .set("StatementType", Strings.padEnd("Select", 20, ' '))
            .set("StatementGroup", "Select")
            .set("StartTime", Instant.parse("2021-07-01T18:15:06Z").toEpochMilli())
            .set("FirstRespTime", Instant.parse("2021-07-01T18:15:08Z").toEpochMilli())
            .set("FirstStepTime", Instant.parse("2021-07-01T18:15:09Z").toEpochMilli())
            .set("Statements", (int) 10)
            .set("NumResultRows", (double) 65.0)
            .set("AMPCPUTime", (double) 1.23)
            .set("AMPCPUTimeNorm", (double) 0.123)
            .set("NumOfActiveAmps", 2)
            .set("MaxStepMemory", (double) 123.45)
            .set("ReqPhysIO", (double) 123.45)
            .set("TotalFirstRespTime", (double) 1234.5)
            .set("TotalIOCount", (double) 1234.56)
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
            .set("DatabaseName", "test_database")
            .set("TableName", "test_table")
            .set("AccessCount", 100_000_000_000L)
            .set("LastAccessTimeStamp", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .set("LastAlterTimeStamp", Instant.parse("2021-07-02T01:00:00Z").toEpochMilli())
            .set("TableKind", "V")
            .set("CreatorName", "creator")
            .set("CreateTimeStamp", Instant.parse("2021-07-02T00:00:00Z").toEpochMilli())
            .set("PrimaryKeyIndexId", 10)
            .set("ParentCount", 2)
            .set("ChildCount", 10)
            .set("CommitOpt", "C")
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
            .set("DataBaseName", "test_database")
            .set("TableName", "test_table")
            .set("CurrentPerm", 1000L)
            .set("PeakPerm", 2000L)
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
            .set("DatabaseName", "test_database")
            .set("TableName", "test_table")
            .set("ColumnName", "test_column")
            .set("ColumnFormat", "test_format")
            .set("ColumnTitle", "test_title")
            .set("ColumnLength", 1000)
            .set("ColumnType", "I ")
            .set("DefaultValue", "0")
            .set("ColumnConstraint", "test constraint")
            .set("ConstraintCount", 1)
            .set("Nullable", "Y")
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
            .set("UserName", "user_1")
            .set("CreatorName", "user_0")
            .set("CreateTimeStamp", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .set("LastAccessTimestamp", null)
            .build();
    GenericRecord expectedRecordUser2 =
        new GenericRecordBuilder(schema)
            .set("UserName", "user_2")
            .set("CreatorName", "user_0")
            .set("CreateTimeStamp", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .set("LastAccessTimestamp", Instant.parse("2021-07-03T02:00:00Z").toEpochMilli())
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
            .set("RoleName", "test_role_1")
            .set("Grantor", "user_0")
            .set("Grantee", "user_1")
            .set("WhenGranted", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .set("DefaultRole", "Y")
            .set("WithAdmin", "N")
            .build();
    GenericRecord expectedRecordRole2 =
        new GenericRecordBuilder(schema)
            .set("RoleName", "test_role_2")
            .set("Grantor", "user_0")
            .set("Grantee", "user_1")
            .set("WhenGranted", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .set("DefaultRole", "N")
            .set("WithAdmin", "Y")
            .build();
    GenericRecord expectedRecordRole3 =
        new GenericRecordBuilder(schema)
            .set("RoleName", "test_role_1")
            .set("Grantor", "user_0")
            .set("Grantee", "user_2")
            .set("WhenGranted", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .set("DefaultRole", "N")
            .set("WithAdmin", "N")
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
            .set("IndexId", 0)
            .set("IndexName", "index_name_0")
            .set("ChildDB", "child_db_0")
            .set("ChildTable", "child_table_0")
            .set("ChildKeyColumn", "child_key_column_0")
            .set("ParentDB", "parent_db_0")
            .set("ParentTable", "parent_table_0")
            .set("ParentKeyColumn", "parent_key_column_0")
            .set("InconsistencyFlag", "Y")
            .set("CreatorName", "creator_0")
            .set("CreateTimeStamp", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .build();
    GenericRecord expectedRecord2 =
        new GenericRecordBuilder(schema)
            .set("IndexId", 1)
            .set("IndexName", "index_name_1")
            .set("ChildDB", "child_db_1")
            .set("ChildTable", "child_table_1")
            .set("ChildKeyColumn", "child_key_column_1")
            .set("ParentDB", "parent_db_0")
            .set("ParentTable", "parent_table_0")
            .set("ParentKeyColumn", "parent_key_column_0")
            .set("InconsistencyFlag", "Y")
            .set("CreatorName", "creator_1")
            .set("CreateTimeStamp", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .build();
    assertThat(records).containsExactly(expectedRecord1, expectedRecord2);

    // Verify records serialization.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    assertThat(readOutputStreamToAvro(outputStream, 2))
        .containsExactly(expectedRecord1, expectedRecord2);
  }

  @Test
  public void loadScripts_partitioning_constraints() throws IOException, SQLException {
    String scriptName = "partitioning_constraints";
    String sqlScript = scriptManager.getScript(scriptName);
    // Get schema and verify records.
    Schema schema = scriptRunner.extractSchema(connection, sqlScript, scriptName, "namespace");
    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, sqlScript, schema);
    GenericRecord expectedRecord =
        new GenericRecordBuilder(schema)
            .set("DatabaseName", "db1")
            .set("IndexName", "index1")
            .set("IndexNumber", 1)
            .set("ConstraintType", "K")
            .set("ConstraintText", "Foo")
            .set("ConstraintCollation", "A")
            .set("CollationName", "ASCII")
            .set("CreatorName", "Creator")
            .set("CreateTimeStamp", Instant.parse("2021-07-01T18:23:42Z").toEpochMilli())
            .set("CharSetID", 5)
            .set("DefinedCombinedPartitions", 2916096L)
            .set("MaxCombinedPartitions", 9223372036854775807L)
            .set("PartitioningLevels", 1)
            .set("ResolvedCurrent_Date", Instant.parse("2021-07-01T00:00:00Z").toEpochMilli())
            .set("ColumnPartitioningLevel", 0)
            .build();
    assertThat(records).containsExactly(expectedRecord);

    // Verify records serialization.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    assertThat(readOutputStreamToAvro(outputStream, /* recordNum= */ 1))
        .containsExactly(expectedRecord);
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
            .set("IndexId", 0)
            .set("IndexName", "index_name_0")
            .set("ChildDB", "child_db_0")
            .set("ChildTable", "child_table_0")
            .set("ChildKeyColumn", "child_key_column_0")
            .set("ParentDB", "parent_db_0")
            .set("ParentTable", "parent_table_0")
            .set("ParentKeyColumn", "parent_key_column_0")
            .set("InconsistencyFlag", "Y")
            .set("CreatorName", "creator_0")
            .set("CreateTimeStamp", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .build();
    GenericRecord expectedRecord2 =
        new GenericRecordBuilder(schema)
            .set("IndexId", 1)
            .set("IndexName", "index_name_1")
            .set("ChildDB", "child_db_1")
            .set("ChildTable", "child_table_1")
            .set("ChildKeyColumn", "child_key_column_1")
            .set("ParentDB", "parent_db_0")
            .set("ParentTable", "parent_table_0")
            .set("ParentKeyColumn", "parent_key_column_0")
            .set("InconsistencyFlag", "Y")
            .set("CreatorName", "creator_1")
            .set("CreateTimeStamp", Instant.parse("2021-07-02T02:00:00Z").toEpochMilli())
            .build();
    assertThat(records).containsExactly(expectedRecord1, expectedRecord2);

    // Verify records serialization.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    assertThat(readOutputStreamToAvro(outputStream, 2))
        .containsExactly(expectedRecord1, expectedRecord2);
  }

  @Test
  public void loadScripts_temptables() throws IOException, SQLException {
    String scriptName = "temp_tables";
    String sqlScript = scriptManager.getScript(scriptName);
    // Get schema and verify records.
    Schema schema = scriptRunner.extractSchema(connection, sqlScript, scriptName, "namespace");
    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, /*sqlScript=*/ sqlScript, schema);
    GenericRecord expectedRecord =
        new GenericRecordBuilder(schema)
            .set("HostNo", 1)
            .set("SessionNo", 1)
            .set("UserName", "user_name")
            .set("B_DatabaseName", "database_name")
            .set("B_TableName", "table_name")
            .set("E_TableId", ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6}))
            .build();
    assertThat(records).containsExactly(expectedRecord);

    // Verify records serialization.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    scriptManager.executeScript(connection, scriptName, new DataEntityManagerTesting(outputStream));
    assertThat(readOutputStreamToAvro(outputStream, 1))
        .containsExactly(expectedRecord);
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
