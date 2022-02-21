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
package com.google.cloud.bigquery.dwhassessment.extractiontool.executor;

import static com.google.cloud.bigquery.dwhassessment.extractiontool.executor.ExtractExecutorImpl.getTeradataTimestampFromInstant;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilter;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaManager.SchemaKey;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SqlTemplateRenderer;
import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.re2j.Pattern;
import java.io.ByteArrayOutputStream;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Properties;
import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public final class ExtractExecutorImplTest {

  private SchemaManager schemaManager;
  private ScriptManager scriptManager;
  private DataEntityManager dataEntityManager;
  private ExtractExecutorImpl executor;
  private Properties properties;

  @Before
  public void setUp() {
    schemaManager = mock(SchemaManager.class);
    scriptManager = mock(ScriptManager.class);
    dataEntityManager = mock(DataEntityManager.class);
    executor = new ExtractExecutorImpl(schemaManager, scriptManager, path -> dataEntityManager);
    properties = new Properties();
    properties.put("user", "");
    properties.put("password", "");
  }

  @Test
  public void run_allScripts_success() throws Exception {
    when(scriptManager.getAllScriptNames()).thenReturn(ImmutableSet.of("one", "two", "three"));
    when(schemaManager.getSchemaKeys(any(Connection.class), eq(ImmutableList.of())))
        .thenReturn(ImmutableSet.of());

    assertThat(
            executor.run(
                ExtractExecutor.Arguments.builder()
                    .setDbConnectionProperties(properties)
                    .setDbConnectionAddress("jdbc:hsqldb:mem:my-animalclinic.example")
                    .setOutputPath(Paths.get("/tmp"))
                    .build()))
        .isEqualTo(0);

    verify(scriptManager).getAllScriptNames();
    verify(scriptManager)
        .executeScript(
            any(Connection.class),
            /*dryRun=*/ eq(false),
            any(SqlTemplateRenderer.class),
            /*scriptName=*/ eq("one"),
            eq(dataEntityManager),
            eq(0));
    verify(scriptManager)
        .executeScript(
            any(Connection.class),
            /*dryRun=*/ eq(false),
            any(SqlTemplateRenderer.class),
            /*scriptName=*/ eq("two"),
            eq(dataEntityManager),
            eq(0));
    verify(scriptManager)
        .executeScript(
            any(Connection.class),
            /*dryRun=*/ eq(false),
            any(SqlTemplateRenderer.class),
            /*scriptName=*/ eq("three"),
            eq(dataEntityManager),
            eq(0));
    verifyNoMoreInteractions(scriptManager);
  }

  @Test
  public void run_overwriteScriptBaseDbAndTableName_success() throws Exception {
    when(scriptManager.getAllScriptNames()).thenReturn(ImmutableSet.of("one", "two"));
    when(schemaManager.getSchemaKeys(any(Connection.class), eq(ImmutableList.of())))
        .thenReturn(ImmutableSet.of());

    assertThat(
            executor.run(
                ExtractExecutor.Arguments.builder()
                    .setDbConnectionProperties(properties)
                    .setDbConnectionAddress("jdbc:hsqldb:mem:my-animalclinic.example")
                    .setOutputPath(Paths.get("/tmp"))
                    .setBaseDatabase("base-db")
                    .setScriptBaseDatabase(ImmutableMap.of("two", "two-db"))
                    .setScriptVariables(
                        ImmutableMap.of("one", ImmutableMap.of("tableName", "one-table")))
                    .build()))
        .isEqualTo(0);

    ArgumentCaptor<SqlTemplateRenderer> sqlTemplateRendererArgumentCaptorOne =
        ArgumentCaptor.forClass(SqlTemplateRenderer.class);
    verify(scriptManager).getAllScriptNames();
    verify(scriptManager)
        .executeScript(
            any(Connection.class),
            /*dryRun=*/ eq(false),
            sqlTemplateRendererArgumentCaptorOne.capture(),
            /*scriptName=*/ eq("one"),eq(dataEntityManager),
            eq(0));
    assertThat(
            sqlTemplateRendererArgumentCaptorOne
                .getValue()
                .renderTemplate("one", "{{baseDatabase}}.{{vars.tableName}}"))
        .isEqualTo("base-db.one-table");

    ArgumentCaptor<SqlTemplateRenderer> sqlTemplateRendererArgumentCaptorTwo =
        ArgumentCaptor.forClass(SqlTemplateRenderer.class);
    verify(scriptManager)
        .executeScript(
            any(Connection.class),
            /*dryRun=*/ eq(false),
            sqlTemplateRendererArgumentCaptorTwo.capture(),
            /*scriptName=*/ eq("two"),eq(dataEntityManager),
            eq(0));
    assertThat(
            sqlTemplateRendererArgumentCaptorTwo
                .getValue()
                .renderTemplate("two", "{{baseDatabase}}.QryLogV"))
        .isEqualTo("two-db.QryLogV");

    verifyNoMoreInteractions(scriptManager);
  }

  @Test
  public void run_selectSomeScripts_success() throws Exception {
    when(scriptManager.getAllScriptNames()).thenReturn(ImmutableSet.of("one", "two", "three"));
    when(schemaManager.getSchemaKeys(any(Connection.class), eq(ImmutableList.of())))
        .thenReturn(ImmutableSet.of());

    assertThat(
            executor.run(
                ExtractExecutor.Arguments.builder()
                    .setDbConnectionProperties(properties)
                    .setDbConnectionAddress("jdbc:hsqldb:mem:my-animalclinic.example")
                    .setOutputPath(Paths.get("/tmp"))
                    .setSqlScripts(ImmutableList.of("one", "three"))
                    .build()))
        .isEqualTo(0);

    verify(scriptManager).getAllScriptNames();
    verify(scriptManager)
        .executeScript(
            any(Connection.class),
            /*dryRun=*/ eq(false),
            any(SqlTemplateRenderer.class),
            /*scriptName=*/ eq("one"),
            eq(dataEntityManager),
            eq(0));
    verify(scriptManager)
        .executeScript(
            any(Connection.class),
            /*dryRun=*/ eq(false),
            any(SqlTemplateRenderer.class),
            /*scriptName=*/ eq("three"),
            eq(dataEntityManager),
            eq(0));
    verifyNoMoreInteractions(scriptManager);
  }

  @Test
  public void run_skipSomeScripts_success() throws Exception {
    when(scriptManager.getAllScriptNames()).thenReturn(ImmutableSet.of("one", "two", "three"));
    when(schemaManager.getSchemaKeys(any(Connection.class), eq(ImmutableList.of())))
        .thenReturn(ImmutableSet.of());

    assertThat(
            executor.run(
                ExtractExecutor.Arguments.builder()
                    .setDbConnectionProperties(properties)
                    .setDbConnectionAddress("jdbc:hsqldb:mem:my-animalclinic.example")
                    .setOutputPath(Paths.get("/tmp"))
                    .setSkipSqlScripts(ImmutableList.of("one", "three"))
                    .build()))
        .isEqualTo(0);

    verify(scriptManager).getAllScriptNames();
    verify(scriptManager)
        .executeScript(
            any(Connection.class),
            /*dryRun=*/ eq(false),
            any(SqlTemplateRenderer.class),
            /*scriptName=*/ eq("two"),
            eq(dataEntityManager),
            eq(0));
    verifyNoMoreInteractions(scriptManager);
  }

  @Test
  public void run_failOnUnknownScripts() {
    when(scriptManager.getAllScriptNames()).thenReturn(ImmutableSet.of("one", "two", "three"));
    when(schemaManager.getSchemaKeys(any(Connection.class), eq(ImmutableList.of())))
        .thenReturn(ImmutableSet.of());

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                executor.run(
                    ExtractExecutor.Arguments.builder()
                        .setDbConnectionProperties(properties)
                        .setDbConnectionAddress("jdbc:hsqldb:mem:my-animalclinic.example")
                        .setOutputPath(Paths.get("/tmp"))
                        .setSqlScripts(ImmutableList.of("four", "five"))
                        .build()));
    assertThat(e).hasMessageThat().contains("Got unknown SQL scripts for sql-scripts: four, five");
  }

  @Test
  public void run_filterScripts_success() throws Exception {
    ImmutableList<SchemaFilter> filters =
        ImmutableList.of(SchemaFilter.builder().setDatabaseName(Pattern.compile("foo")).build());
    when(scriptManager.getAllScriptNames()).thenReturn(ImmutableSet.of());
    when(schemaManager.getSchemaKeys(any(Connection.class), eq(filters)))
        .thenReturn(
            ImmutableSet.of(SchemaKey.create("foo", "bar"), SchemaKey.create("foo", "baz")));
    when(schemaManager.retrieveSchema(any(Connection.class), any(), any(Schema.class)))
        .thenReturn(ImmutableList.of());
    when(dataEntityManager.getEntityOutputStream("schema.avro"))
        .thenReturn(new ByteArrayOutputStream());

    assertThat(
            executor.run(
                ExtractExecutor.Arguments.builder()
                    .setDbConnectionProperties(properties)
                    .setDbConnectionAddress("jdbc:hsqldb:mem:my-animalclinic.example")
                    .setOutputPath(Paths.get("/tmp"))
                    .setSchemaFilters(filters)
                    .build()))
        .isEqualTo(0);

    verify(schemaManager).getSchemaKeys(any(Connection.class), eq(filters));
    verify(schemaManager)
        .retrieveSchema(
            any(Connection.class), eq(SchemaKey.create("foo", "baz")), any(Schema.class));
    verify(schemaManager)
        .retrieveSchema(
            any(Connection.class), eq(SchemaKey.create("foo", "bar")), any(Schema.class));
    verifyNoMoreInteractions(schemaManager);
  }

  @Test
  public void run_failOnUnknownSkipScripts() {
    when(scriptManager.getAllScriptNames()).thenReturn(ImmutableSet.of("one", "two", "three"));
    when(schemaManager.getSchemaKeys(any(Connection.class), eq(ImmutableList.of())))
        .thenReturn(ImmutableSet.of());

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                executor.run(
                    ExtractExecutor.Arguments.builder()
                        .setDbConnectionProperties(properties)
                        .setDbConnectionAddress("jdbc:hsqldb:mem:my-animalclinic.example")
                        .setOutputPath(Paths.get("/tmp"))
                        .setSkipSqlScripts(ImmutableList.of("four", "five"))
                        .build()));
    assertThat(e)
        .hasMessageThat()
        .contains("Got unknown SQL scripts for skip-sql-scripts: four, five");
  }

  @Test
  public void getTeradataTimestampFromInstant_outputShouldBeCorrect() {
    assertThat(getTeradataTimestampFromInstant(Instant.parse("2022-01-24T14:52:00Z")))
        .isEqualTo("2022-01-24 14:52:00.000000");
    assertThat(getTeradataTimestampFromInstant(Instant.parse("2022-01-24T14:52:00.123456Z")))
        .isEqualTo("2022-01-24 14:52:00.123456");
  }

  @Test
  public void getTeradataTimestampFromInstant_timeBoundaries() {
    assertThat(getTeradataTimestampFromInstant(Instant.parse("2022-10-01T00:00:00Z")))
        .isEqualTo("2022-10-01 00:00:00.000000");
    assertThat(getTeradataTimestampFromInstant(Instant.parse("2022-10-01T24:00:00Z")))
        .isEqualTo("2022-10-02 00:00:00.000000");
  }
}
