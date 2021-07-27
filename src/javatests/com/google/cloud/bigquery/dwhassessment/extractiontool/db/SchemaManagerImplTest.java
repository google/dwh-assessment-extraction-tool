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

import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.AvroHelper.getAvroSchema;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaManager.SchemaKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.re2j.Pattern;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class SchemaManagerImplTest {

  private static SchemaManager schemaManager;
  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    schemaManager = new SchemaManagerImpl();
    connection = DriverManager.getConnection("jdbc:hsqldb:mem:db_0");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute("CREATE TABLE FOO (ID VARCHAR(1), NAME VARCHAR(100))");
    baseStmt.execute("CREATE TABLE FOOBAR (ID INTEGER, NAME VARCHAR(100))");
    baseStmt.execute("CREATE TABLE BAR (ID INTEGER, NAME VARCHAR(100))");
    baseStmt.close();
    connection.commit();
  }

  @Test
  public void getSchemaKeys_onlyTableFilter_success() {
    ImmutableSet<SchemaKey> results = schemaManager.getSchemaKeys(connection, ImmutableList.of());

    assertThat(results)
        .containsAtLeastElementsIn(
            ImmutableSet.of(
                SchemaKey.create("HSQL Database Engine", "FOO"),
                SchemaKey.create("HSQL Database Engine", "FOOBAR"),
                SchemaKey.create("HSQL Database Engine", "BAR")));
  }

  @Test
  public void getSchemaKeys_filterBothDatabaseAndTables_success() {
    ImmutableSet<SchemaKey> results =
        schemaManager.getSchemaKeys(
            connection,
            ImmutableList.of(
                SchemaFilter.builder()
                    .setDatabaseName(Pattern.compile("HSQL.*"))
                    .setTableName(Pattern.compile("FOO.*"))
                    .build()));

    assertThat(results)
        .containsAtLeastElementsIn(
            ImmutableSet.of(
                SchemaKey.create("HSQL Database Engine", "FOO"),
                SchemaKey.create("HSQL Database Engine", "FOOBAR")));
  }

  @Test
  public void getSchemaKeys_filterOnlyDatabase_success() {
    ImmutableSet<SchemaKey> results =
        schemaManager.getSchemaKeys(
            connection,
            ImmutableList.of(
                SchemaFilter.builder().setDatabaseName(Pattern.compile("HSQL.*")).build()));

    assertThat(results)
        .containsAtLeastElementsIn(
            ImmutableSet.of(
                SchemaKey.create("HSQL Database Engine", "FOO"),
                SchemaKey.create("HSQL Database Engine", "FOOBAR"),
                SchemaKey.create("HSQL Database Engine", "BAR")));
  }

  @Test
  public void getSchemaKeys_filterJustTableName_success() {
    ImmutableSet<SchemaKey> results =
        schemaManager.getSchemaKeys(
            connection,
            ImmutableList.of(
                SchemaFilter.builder().setTableName(Pattern.compile("FOO.*")).build()));

    assertThat(results)
        .containsAtLeastElementsIn(
            ImmutableSet.of(
                SchemaKey.create("HSQL Database Engine", "FOO"),
                SchemaKey.create("HSQL Database Engine", "FOOBAR")));
  }

  @Test
  public void retrieveSchemaTest() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    String databaseName = "HSQL Database Engine";
    String tableName = "FOO";
    Schema schema =
        getAvroSchema(
            "schema",
            "namespace",
            connection
                .getMetaData()
                .getColumns(
                    /*catalog =*/ null,
                    /*schemaPattern =*/ null,
                    /*tableNamePattern =*/ "%",
                    /*columnNamePattern =*/ null)
                .getMetaData());

    ImmutableList<GenericRecord> records =
        schemaManager.retrieveSchema(connection, SchemaKey.create(databaseName, tableName), schema);

    GenericRecord expectedRecord =
        new GenericRecordBuilder(schema)
            .set("TABLE_CAT", "PUBLIC")
            .set("TABLE_SCHEM", "PUBLIC")
            .set("TABLE_NAME", "FOO")
            .set("COLUMN_NAME", "ID")
            .set("DATA_TYPE", 12)
            .set("TYPE_NAME", "VARCHAR")
            .set("COLUMN_SIZE", 1)
            .set("BUFFER_LENGTH", null)
            .set("DECIMAL_DIGITS", null)
            .set("NUM_PREC_RADIX", null)
            .set("NULLABLE", 1)
            .set("REMARKS", null)
            .set("COLUMN_DEF", null)
            .set("SQL_DATETIME_SUB", null)
            .set("CHAR_OCTET_LENGTH", 1)
            .set("ORDINAL_POSITION", 1)
            .set("IS_NULLABLE", "YES")
            .set("SCOPE_CATALOG", null)
            .set("SCOPE_SCHEMA", null)
            .set("SCOPE_TABLE", null)
            .set("SOURCE_DATA_TYPE", null)
            .set("IS_AUTOINCREMENT", "NO")
            .set("IS_GENERATEDCOLUMN", "NO")
            .set("SQL_DATA_TYPE", 12)
            .build();
    GenericRecord expectedSecondRecord =
        new GenericRecordBuilder(schema)
            .set("TABLE_CAT", "PUBLIC")
            .set("TABLE_SCHEM", "PUBLIC")
            .set("TABLE_NAME", "FOO")
            .set("COLUMN_NAME", "NAME")
            .set("DATA_TYPE", 12)
            .set("TYPE_NAME", "VARCHAR")
            .set("COLUMN_SIZE", 100)
            .set("BUFFER_LENGTH", null)
            .set("DECIMAL_DIGITS", null)
            .set("NUM_PREC_RADIX", null)
            .set("NULLABLE", 1)
            .set("REMARKS", null)
            .set("COLUMN_DEF", null)
            .set("SQL_DATETIME_SUB", null)
            .set("CHAR_OCTET_LENGTH", 100)
            .set("ORDINAL_POSITION", 2)
            .set("IS_NULLABLE", "YES")
            .set("SCOPE_CATALOG", null)
            .set("SCOPE_SCHEMA", null)
            .set("SCOPE_TABLE", null)
            .set("SOURCE_DATA_TYPE", null)
            .set("IS_AUTOINCREMENT", "NO")
            .set("IS_GENERATEDCOLUMN", "NO")
            .set("SQL_DATA_TYPE", 12)
            .build();
    assertThat(records).containsExactly(expectedRecord, expectedSecondRecord);
  }
}
