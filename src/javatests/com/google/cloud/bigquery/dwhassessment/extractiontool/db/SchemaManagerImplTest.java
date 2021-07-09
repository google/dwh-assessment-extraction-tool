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

import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaManager.SchemaKey;
import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManagerTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.re2j.Pattern;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
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
    baseStmt.execute("CREATE TABLE FOO (ID INTEGER, NAME VARCHAR(100))");
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
    DataEntityManager dataEntityManager = new DataEntityManagerTesting(outputStream);
    String databaseName = "HSQL Database Engine";
    String tableName = "FOO";

    schemaManager.retrieveSchema(
        connection, SchemaKey.create(databaseName, tableName), dataEntityManager);

    // Using a string instead of using AvroHelper.getAvroSchema() because DECIMAL_DIGITS has
    // type INTEGER but requires type long in AVRO, else it would give
    // "org.apache.avro.UnresolvedUnionException: Not in union [“null”,“int”]" error.
    String expectedSchema =
        "{\"type\":\"record\",\"name\":\"FOO\",\"namespace\":\"HSQL Database Engine\",\"fields\":["
            + "{\"name\":\"TABLE_CAT\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"TABLE_SCHEM\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"TABLE_NAME\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"COLUMN_NAME\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"DATA_TYPE\",\"type\":[\"null\",\"int\"],\"default\":null},"
            + "{\"name\":\"TYPE_NAME\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"COLUMN_SIZE\",\"type\":[\"null\",\"int\"],\"default\":null},"
            + "{\"name\":\"BUFFER_LENGTH\",\"type\":[\"null\",\"int\"],\"default\":null},"
            + "{\"name\":\"DECIMAL_DIGITS\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"NUM_PREC_RADIX\",\"type\":[\"null\",\"int\"],\"default\":null},"
            + "{\"name\":\"NULLABLE\",\"type\":[\"null\",\"int\"],\"default\":null},"
            + "{\"name\":\"REMARKS\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"COLUMN_DEF\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"SQL_DATA_TYPE\",\"type\":[\"null\",\"int\"],\"default\":null},"
            + "{\"name\":\"SQL_DATETIME_SUB\",\"type\":[\"null\",\"int\"],\"default\":null},"
            + "{\"name\":\"CHAR_OCTET_LENGTH\",\"type\":[\"null\",\"int\"],\"default\":null},"
            + "{\"name\":\"ORDINAL_POSITION\",\"type\":[\"null\",\"int\"],\"default\":null},"
            + "{\"name\":\"IS_NULLABLE\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"SCOPE_CATALOG\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"SCOPE_SCHEMA\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"SCOPE_TABLE\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"SOURCE_DATA_TYPE\",\"type\":[\"null\",\"int\"],\"default\":null},"
            + "{\"name\":\"IS_AUTOINCREMENT\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"IS_GENERATEDCOLUMN\",\"type\":[\"null\",\"string\"],\"default\":null}"
            + "]}\n";
    Schema schema = new Schema.Parser().parse(expectedSchema);
    GenericDatumReader<Record> reader = new GenericDatumReader<>(schema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
    Record result = reader.read(null, decoder);
    GenericRecord expectedRecord =
        new GenericRecordBuilder(schema)
            .set("TABLE_CAT", "PUBLIC")
            .set("TABLE_SCHEM", "PUBLIC")
            .set("TABLE_NAME", "FOO")
            .set("COLUMN_NAME", "ID")
            .set("DATA_TYPE", 4)
            .set("TYPE_NAME", "INTEGER")
            .set("COLUMN_SIZE", 32)
            .set("BUFFER_LENGTH", null)
            .set("DECIMAL_DIGITS", (long) 0)
            .set("NUM_PREC_RADIX", 2)
            .set("NULLABLE", 1)
            .set("REMARKS", null)
            .set("COLUMN_DEF", null)
            .set("SQL_DATETIME_SUB", null)
            .set("CHAR_OCTET_LENGTH", 0)
            .set("ORDINAL_POSITION", 1)
            .set("IS_NULLABLE", "YES")
            .set("SCOPE_CATALOG", null)
            .set("SCOPE_SCHEMA", null)
            .set("SCOPE_TABLE", null)
            .set("SOURCE_DATA_TYPE", null)
            .set("IS_AUTOINCREMENT", "NO")
            .set("IS_GENERATEDCOLUMN", "NO")
            .set("SQL_DATA_TYPE", 4)
            .build();
    assertThat(result).isEqualTo(expectedRecord);
    Record secondResult = reader.read(null, decoder);
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
    assertThat(secondResult).isEqualTo(expectedSecondRecord);
    assertThrows(IOException.class, () -> reader.read(null, decoder));
  }
}
