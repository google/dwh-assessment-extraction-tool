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
 * package com.google.cloud.bigquery.dwhassessment.extractiontool.config;
 */
package com.google.cloud.bigquery.dwhassessment.extractiontool.executor;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilter;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaManager.SchemaKey;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.re2j.Pattern;
import java.nio.file.Paths;
import java.sql.Connection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ExtractExecutorImplTest {

  private Connection connection;
  private SchemaManager schemaManager;
  private ScriptManager scriptManager;
  private DataEntityManager dataEntityManager;
  private ExtractExecutorImpl executor;

  @Before
  public void setUp() {
    connection = mock(Connection.class);
    schemaManager = mock(SchemaManager.class);
    scriptManager = mock(ScriptManager.class);
    dataEntityManager = mock(DataEntityManager.class);
    executor =
        new ExtractExecutorImpl(
            address -> connection, schemaManager, scriptManager, path -> dataEntityManager);
  }

  @Test
  public void run_allScripts_success() {
    when(scriptManager.getAllScriptNames()).thenReturn(ImmutableSet.of("one", "two", "three"));
    when(schemaManager.getSchemaKeys(connection, ImmutableList.of())).thenReturn(ImmutableSet.of());

    assertThat(
            executor.run(
                ExtractExecutor.Arguments.builder()
                    .setDbAddress("jdbc:fake")
                    .setOutputPath(Paths.get("/tmp"))
                    .build()))
        .isEqualTo(0);

    verify(scriptManager).getAllScriptNames();
    verify(scriptManager).executeScript(connection, /*scriptName=*/ "one", dataEntityManager);
    verify(scriptManager).executeScript(connection, /*scriptName=*/ "two", dataEntityManager);
    verify(scriptManager).executeScript(connection, /*scriptName=*/ "three", dataEntityManager);
    verifyNoMoreInteractions(scriptManager);
  }

  @Test
  public void run_selectSomeScripts_success() {
    when(scriptManager.getAllScriptNames()).thenReturn(ImmutableSet.of("one", "two", "three"));
    when(schemaManager.getSchemaKeys(connection, ImmutableList.of())).thenReturn(ImmutableSet.of());

    assertThat(
            executor.run(
                ExtractExecutor.Arguments.builder()
                    .setDbAddress("jdbc:fake")
                    .setOutputPath(Paths.get("/tmp"))
                    .setSqlScripts(ImmutableList.of("one", "three"))
                    .build()))
        .isEqualTo(0);

    verify(scriptManager).getAllScriptNames();
    verify(scriptManager).executeScript(connection, /*scriptName=*/ "one", dataEntityManager);
    verify(scriptManager).executeScript(connection, /*scriptName=*/ "three", dataEntityManager);
    verifyNoMoreInteractions(scriptManager);
  }

  @Test
  public void run_skipSomeScripts_success() {
    when(scriptManager.getAllScriptNames()).thenReturn(ImmutableSet.of("one", "two", "three"));
    when(schemaManager.getSchemaKeys(connection, ImmutableList.of())).thenReturn(ImmutableSet.of());

    assertThat(
            executor.run(
                ExtractExecutor.Arguments.builder()
                    .setDbAddress("jdbc:fake")
                    .setOutputPath(Paths.get("/tmp"))
                    .setSkipSqlScripts(ImmutableList.of("one", "three"))
                    .build()))
        .isEqualTo(0);

    verify(scriptManager).getAllScriptNames();
    verify(scriptManager).executeScript(connection, /*scriptName=*/ "two", dataEntityManager);
    verifyNoMoreInteractions(scriptManager);
  }

  @Test
  public void run_failOnUnknownScripts() {
    when(scriptManager.getAllScriptNames()).thenReturn(ImmutableSet.of("one", "two", "three"));
    when(schemaManager.getSchemaKeys(connection, ImmutableList.of())).thenReturn(ImmutableSet.of());

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                executor.run(
                    ExtractExecutor.Arguments.builder()
                        .setDbAddress("jdbc:fake")
                        .setOutputPath(Paths.get("/tmp"))
                        .setSqlScripts(ImmutableList.of("four", "five"))
                        .build()));
    assertThat(e).hasMessageThat().contains("Got unknown SQL scripts for sql-scripts: four, five");
  }

  @Test
  public void run_filterScripts_success() {
    ImmutableList<SchemaFilter> filters =
        ImmutableList.of(SchemaFilter.builder().setDatabaseName(Pattern.compile("foo")).build());
    when(scriptManager.getAllScriptNames()).thenReturn(ImmutableSet.of());
    when(schemaManager.getSchemaKeys(connection, filters))
        .thenReturn(
            ImmutableSet.of(SchemaKey.create("foo", "bar"), SchemaKey.create("foo", "baz")));

    assertThat(
            executor.run(
                ExtractExecutor.Arguments.builder()
                    .setDbAddress("jdbc:fake")
                    .setOutputPath(Paths.get("/tmp"))
                    .setSchemaFilters(filters)
                    .build()))
        .isEqualTo(0);

    verify(schemaManager).getSchemaKeys(connection, filters);
    verify(schemaManager)
        .retrieveSchema(connection, SchemaKey.create("foo", "bar"), dataEntityManager);
    verify(schemaManager)
        .retrieveSchema(connection, SchemaKey.create("foo", "baz"), dataEntityManager);
    verifyNoMoreInteractions(schemaManager);
  }

  @Test
  public void run_failOnUnknownSkipScripts() {
    when(scriptManager.getAllScriptNames()).thenReturn(ImmutableSet.of("one", "two", "three"));
    when(schemaManager.getSchemaKeys(connection, ImmutableList.of())).thenReturn(ImmutableSet.of());

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                executor.run(
                    ExtractExecutor.Arguments.builder()
                        .setDbAddress("jdbc:fake")
                        .setOutputPath(Paths.get("/tmp"))
                        .setSkipSqlScripts(ImmutableList.of("four", "five"))
                        .build()));
    assertThat(e)
        .hasMessageThat()
        .contains("Got unknown SQL scripts for skip-sql-scripts: four, five");
  }
}
