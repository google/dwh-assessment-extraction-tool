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

import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaManager.SchemaKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.re2j.Pattern;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
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
}
