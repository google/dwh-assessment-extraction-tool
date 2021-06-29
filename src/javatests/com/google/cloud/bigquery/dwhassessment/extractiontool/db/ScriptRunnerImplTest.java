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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ScriptRunnerImplTest {

  private ScriptRunner scriptRunner;

  private final String TEST_SCHEMA =
      "{\"namespace\": \"test.schema\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"test_name\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"ID\", \"type\": [\"int\", \"null\"]},\n"
          + "     {\"name\": \"NAME\", \"type\": [\"string\", \"null\"]}\n"
          + " ]\n"
          + "}";

  @Before
  public void setUp() {
    scriptRunner = new ScriptRunnerImpl();
  }

  @Test
  public void provided_schema_simple_script() throws SQLException, IOException {
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:db_0");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute("CREATE TABLE T0 (" + "ID INTEGER," + "NAME VARCHAR(100)" + ")");
    baseStmt.execute("INSERT INTO T0 VALUES (0, 'name_0')");
    baseStmt.close();
    connection.commit();

    Schema testSchema = new Schema.Parser().parse(TEST_SCHEMA);
    String sqlScript = "SELECT * FROM T0";
    ImmutableList<GenericRecord> records = scriptRunner
        .executeScriptToAvro(connection, sqlScript, testSchema);

    GenericRecord expectedRecord = new GenericRecordBuilder(testSchema)
        .set("ID", 0).set("NAME", "name_0").build();
    assertThat(records).containsExactly(expectedRecord);
  }

  @Test
  public void extract_schema_simple_script() throws SQLException, IOException {
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:db_1");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute("CREATE TABLE T0 (" + "ID INTEGER," + "NAME VARCHAR(100)" + ")");
    baseStmt.execute("INSERT INTO T0 VALUES (0, 'name_0')");
    baseStmt.close();
    connection.commit();

    String sqlScript = "SELECT * FROM T0";
    Schema testSchema = scriptRunner
        .extractSchema(connection, sqlScript, "testName", "namespace");
    ImmutableList<GenericRecord> records = scriptRunner
        .executeScriptToAvro(connection, sqlScript, testSchema);

    GenericRecord expectedRecord = new GenericRecordBuilder(testSchema)
        .set("ID", 0).set("NAME", "name_0").build();
    assertThat(records).containsExactly(expectedRecord);
  }

  @Test
  public void null_values_success() throws Exception, SQLException {
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:test_db");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute("CREATE TABLE T1 (" + "ID INTEGER," + "NAME VARCHAR(100)" + ")");
    baseStmt.execute("INSERT INTO T1 VALUES (0, 'name_0')");
    baseStmt.execute("INSERT INTO T1 VALUES (1, null)");
    baseStmt.execute("INSERT INTO T1 VALUES (null, 'name_2')");
    baseStmt.close();
    connection.commit();

    Schema testSchema = new Schema.Parser().parse(TEST_SCHEMA);
    String sqlScript = "SELECT * FROM T1";
    ImmutableList<GenericRecord> records = scriptRunner
        .executeScriptToAvro(connection, sqlScript, testSchema);

    GenericRecord expectedRecord0 = new GenericRecordBuilder(testSchema)
        .set("ID", 0).set("NAME", "name_0").build();
    GenericRecord expectedRecord1 = new GenericRecordBuilder(testSchema)
        .set("ID", 1).set("NAME", null).build();
    GenericRecord expectedRecord2 = new GenericRecordBuilder(testSchema)
        .set("ID", null).set("NAME", "name_2").build();
    assertThat(records).containsExactly(expectedRecord0, expectedRecord1, expectedRecord2);
  }
}
