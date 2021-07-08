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
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManagerTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ScriptManagerImplTest {

  private ScriptManager scriptManager;
  private DataEntityManager dataEntityManager;
  private ByteArrayOutputStream outputStream;
  private ScriptRunner scriptRunner;
  private final ImmutableMap<String, Supplier<String>> scriptsMap =
      ImmutableMap.of("default", () -> "SELECT * FROM TestTable");

  @Before
  public void setUp() throws Exception {
    scriptRunner = new ScriptRunnerImpl();
    outputStream = new ByteArrayOutputStream();
    dataEntityManager = new DataEntityManagerTesting(outputStream);
  }

  @Test
  public void executeScript_simpleTable_success() throws Exception, SQLException {
    scriptManager = new ScriptManagerImpl(scriptRunner, scriptsMap);
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:db_0");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute("CREATE Table TestTable (" + "ID INTEGER," + "NAME VARCHAR(100)" + ")");
    baseStmt.execute("INSERT INTO TestTable VALUES (0, 'name_0')");
    baseStmt.close();
    connection.commit();
    scriptManager.executeScript(connection, "default", dataEntityManager);

    String sqlScript = "SELECT * FROM TestTable";
    Schema testSchema =
        scriptRunner.extractSchema(connection, sqlScript, "schemaName", "namespace");
    GenericDatumReader<Record> reader = new GenericDatumReader<>(testSchema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
    Record result = reader.read(null, decoder);
    GenericRecord expectedRecord =
        new GenericRecordBuilder(testSchema).set("ID", 0).set("NAME", "name_0").build();
    assertThat(result).isEqualTo(expectedRecord);
  }

  @Test
  public void executeScript_emptyTable_success() throws Exception, SQLException {
    scriptManager = new ScriptManagerImpl(scriptRunner, scriptsMap);
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:db_1");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute("CREATE Table TestTable (" + "ID INTEGER," + "NAME VARCHAR(100)" + ")");
    baseStmt.close();
    connection.commit();
    scriptManager.executeScript(connection, "default", dataEntityManager);

    String sqlScript = "SELECT * FROM TestTable";
    Schema testSchema =
        scriptRunner.extractSchema(connection, sqlScript, "schemaName", "namespace");
    assertThat(outputStream.toByteArray().length).isEqualTo(0);
  }

  @Test
  public void getAllScriptNames_fail() throws Exception, SQLException {
    scriptManager = new ScriptManagerImpl(scriptRunner, scriptsMap);
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:db_2");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute("CREATE Table TestTable (" + "ID INTEGER," + "NAME VARCHAR(100)" + ")");
    baseStmt.execute("INSERT INTO TestTable VALUES (0, 'name_0')");
    baseStmt.close();
    connection.commit();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            scriptManager.executeScript(connection, "not_existing_script_name", dataEntityManager));
  }

  @Test
  public void getAllScriptNames_success() {
    scriptManager = new ScriptManagerImpl(scriptRunner, scriptsMap);
    assertThat(scriptManager.getAllScriptNames()).isEqualTo(ImmutableSet.of("default"));
  }

  @Test
  public void getScript_success() {
    scriptManager = new ScriptManagerImpl(scriptRunner, scriptsMap);
    assertThat(scriptManager.getScript("default")).isEqualTo(scriptsMap.get("default").get());
  }

  @Test
  public void getScript_fail() {
    scriptManager = new ScriptManagerImpl(scriptRunner, scriptsMap);
    assertThrows(
        IllegalArgumentException.class, () -> scriptManager.getScript("not_available_name"));
  }
}
