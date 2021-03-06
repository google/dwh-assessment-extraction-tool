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
package com.google.cloud.bigquery.dwhassessment.extractiontool.subcommand;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilter;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilters;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptManagerImpl;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptRunnerImpl;
import com.google.cloud.bigquery.dwhassessment.extractiontool.executor.ExtractExecutor;
import com.google.cloud.bigquery.dwhassessment.extractiontool.executor.ExtractExecutor.RunMode;
import com.google.common.collect.ImmutableMap;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Instant;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import picocli.CommandLine;

@RunWith(JUnit4.class)
public final class ExtractSubcommandTest {

  private static Path outputPath;
  private static Path prevRunPath;

  private final ScriptManager scriptManager =
      new ScriptManagerImpl(
          new ScriptRunnerImpl(),
          ImmutableMap.of("one", () -> "", "two", () -> "", "querylogs", () -> ""),
          ImmutableMap.of());

  @BeforeClass
  public static void setUpClass() throws IOException {
    outputPath = Files.createTempDirectory("extract-test");
    prevRunPath = Files.createTempDirectory("prev-extract-test");
  }

  @Test
  public void call_success() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db1.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString()))
        .isEqualTo(0);

    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);
    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();

    assertThat(arguments.outputPath().toString()).isEqualTo(outputPath.toString());
    assertThat(arguments.sqlScripts()).isEmpty();
    assertThat(arguments.skipSqlScripts()).isEmpty();
    assertThat(arguments.chunkRows()).isEqualTo(0);
    assertThat(arguments.needQueryText()).isTrue();
  }

  @Test
  public void call_successWithChunkRows() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db2.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--rows-per-chunk",
                "5000"))
        .isEqualTo(0);

    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);
    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();

    assertThat(arguments.outputPath().toString()).isEqualTo(outputPath.toString());
    assertThat(arguments.sqlScripts()).isEmpty();
    assertThat(arguments.skipSqlScripts()).isEmpty();
    assertThat(arguments.chunkRows()).isEqualTo(5000);
  }

  @Test
  public void call_successWithSqlScripts() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db3.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--sql-scripts",
                "one,two, three"))
        .isEqualTo(0);

    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);
    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();

    assertThat(arguments.outputPath().toString()).isEqualTo(outputPath.toString());
    assertThat(arguments.sqlScripts()).containsExactly("one", "two", "three").inOrder();
    assertThat(arguments.skipSqlScripts()).isEmpty();
    assertThat(arguments.schemaFilters()).isEmpty();
  }

  @Test
  public void call_successWithNoNeedQueryText() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-noneedquerytext",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--need-querytext=false"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.needQueryText()).isFalse();
  }

  @Test
  public void call_successWithNegatedNeedQueryText() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-negatedneedquerytext",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--no-need-querytext"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.needQueryText()).isFalse();
  }

  @Test
  public void call_successWithNoNeedJdbcSchemas() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-noneedjdbcschemas",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--need-jdbc-schemas=false"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.needJdbcSchemas()).isFalse();
  }

  @Test
  public void call_successWithNegatedNeedJdbcSchemas() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-negatedneedjdbcschemas",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--no-need-jdbc-schemas"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.needJdbcSchemas()).isFalse();
  }

  @Test
  public void call_successWithSkipSqlScripts() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db4.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--skip-sql-scripts",
                "one,two, three"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.outputPath().toString()).isEqualTo(outputPath.toString());
    assertThat(arguments.sqlScripts()).isEmpty();
    assertThat(arguments.skipSqlScripts()).containsExactly("one", "two", "three").inOrder();
    assertThat(arguments.schemaFilters()).isEmpty();
  }

  @Test
  public void call_successWithSchemaFilters() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd =
        new CommandLine(new ExtractSubcommand(() -> executor, scriptManager))
            .registerConverter(SchemaFilter.class, SchemaFilters::parse);
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db5.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--schema-filter",
                "db:foo"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.outputPath().toString()).isEqualTo(outputPath.toString());
    assertThat(arguments.sqlScripts()).isEmpty();
    assertThat(arguments.skipSqlScripts()).isEmpty();
    assertThat(arguments.schemaFilters())
        .containsExactly(SchemaFilter.builder().setDatabaseName(Pattern.compile("foo")).build())
        .inOrder();
  }

  @Test
  public void call_successWithTimeRangeStartDatetime() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db6.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--qrylog-timerange-start",
                "2021-01-01T11:46:46.42134212"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.outputPath().toString()).isEqualTo(outputPath.toString());
    assertThat(arguments.skipSqlScripts()).isEmpty();
    assertThat(arguments.schemaFilters()).isEmpty();
    assertThat(arguments.qryLogStartTime())
        .hasValue(Instant.parse("2021-01-01T11:46:46.42134212Z"));
    assertThat(arguments.qryLogEndTime()).isEmpty();
  }

  @Test
  public void call_successWithTimeRangeStartDate() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db7.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--qrylog-timerange-start",
                "2021-01-01"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.outputPath().toString()).isEqualTo(outputPath.toString());
    assertThat(arguments.skipSqlScripts()).isEmpty();
    assertThat(arguments.schemaFilters()).isEmpty();
    assertThat(arguments.qryLogStartTime()).hasValue(Instant.parse("2021-01-01T00:00:00.00Z"));
    assertThat(arguments.qryLogEndTime()).isEmpty();
  }

  @Test
  public void call_successWithEndTimeRangeCustomTimezone() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db8.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--qrylog-timerange-end",
                "2021-01-01T11:46:46.42134212",
                "--time-zone",
                "Europe/Warsaw"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.outputPath().toString()).isEqualTo(outputPath.toString());
    assertThat(arguments.skipSqlScripts()).isEmpty();
    assertThat(arguments.schemaFilters()).isEmpty();
    assertThat(arguments.qryLogStartTime()).isEmpty();
    assertThat(arguments.qryLogEndTime()).hasValue(Instant.parse("2021-01-01T10:46:46.42134212Z"));
  }

  @Test
  public void call_successWithTimeRangeStartDatetimeEndDateCustomTimezone()
      throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db9.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--qrylog-timerange-start",
                "2021-01-01T11:46:46.42134212",
                "--qrylog-timerange-end",
                "2022-01-01",
                "--time-zone",
                "Europe/Warsaw"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();

    assertThat(arguments.outputPath().toString()).isEqualTo(outputPath.toString());
    assertThat(arguments.skipSqlScripts()).isEmpty();
    assertThat(arguments.schemaFilters()).isEmpty();
    assertThat(arguments.qryLogStartTime())
        .hasValue(Instant.parse("2021-01-01T10:46:46.42134212Z"));
    assertThat(arguments.qryLogEndTime()).hasValue(Instant.parse("2021-12-31T23:00:00Z"));
  }

  @Test
  public void call_successWithTimeRangeDespiteStartIsLaterThenEnd()
      throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db10.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--qrylog-timerange-start",
                "2022-01-01T11:46:46.42134212",
                "--qrylog-timerange-end",
                "2021-01-01T11:46:46.42134212",
                "--time-zone",
                "Europe/Warsaw"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.outputPath().toString()).isEqualTo(outputPath.toString());
    assertThat(arguments.skipSqlScripts()).isEmpty();
    assertThat(arguments.schemaFilters()).isEmpty();
    assertThat(arguments.qryLogStartTime())
        .hasValue(Instant.parse("2022-01-01T10:46:46.42134212Z"));
    assertThat(arguments.qryLogEndTime()).hasValue(Instant.parse("2021-01-01T10:46:46.42134212Z"));
  }

  @Test
  public void call_successWithScriptBaseDb() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-script-base.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--script-base-db",
                "one=Foo,two=Bar"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.scriptBaseDatabase()).containsExactly("one", "Foo", "two", "Bar");
  }

  @Test
  public void call_successWithQryLogUserFilter() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-qrylog-user.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--qrylog-users",
                "one,two"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.qryLogUsers()).containsExactly("one", "two");
  }

  @Test
  public void call_successWithScriptVars() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-script-var.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--script-vars",
                "one.foo=Bar,one.foo1=Bar1,two.foo=Bar2"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.scriptVariables())
        .containsExactly(
            "one",
            ImmutableMap.of("foo", "Bar", "foo1", "Bar1"),
            "two",
            ImmutableMap.of("foo", "Bar2"));
  }

  @Test
  public void call_successWithIncrementalMode() throws SQLException, IOException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-inc-success.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--run-mode",
                "INCREMENTAL",
                "--rows-per-chunk",
                "5000",
                "--prev-run-path",
                prevRunPath.toString()))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.outputPath().toString()).isEqualTo(outputPath.toString());
    assertThat(arguments.mode()).isEqualTo(RunMode.INCREMENTAL);
    assertThat(arguments.prevRunPath().orElse(Paths.get("")).toString())
        .isEqualTo(prevRunPath.toString());
    assertThat(arguments.sqlScripts()).isEmpty();
    assertThat(arguments.skipSqlScripts()).isEmpty();
    assertThat(arguments.chunkRows()).isEqualTo(5000);
  }

  @Test
  public void call_successWithRecoveryMode() throws SQLException, IOException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-recovery-success.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--run-mode",
                "RECOVERY",
                "--rows-per-chunk",
                "5000",
                "--prev-run-path",
                prevRunPath.toString()))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.outputPath().toString()).isEqualTo(outputPath.toString());
    assertThat(arguments.prevRunPath().orElse(Paths.get("")).toString())
        .isEqualTo(prevRunPath.toString());
    assertThat(arguments.sqlScripts()).isEmpty();
    assertThat(arguments.skipSqlScripts()).isEmpty();
    assertThat(arguments.chunkRows()).isEqualTo(5000);
    assertThat(arguments.mode()).isEqualTo(RunMode.RECOVERY);
  }

  @Test
  public void call_successWithRecoveryModeButNoPrevRunPath() throws SQLException, IOException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-recovery-noprevrun-success.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--run-mode",
                "RECOVERY",
                "--rows-per-chunk",
                "5000"))
        .isEqualTo(0);

    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();
    assertThat(arguments.outputPath().toString()).isEqualTo(outputPath.toString());
    assertThat(arguments.prevRunPath().orElse(Paths.get("")).toString())
        .isEqualTo(outputPath.toString());
    assertThat(arguments.sqlScripts()).isEmpty();
    assertThat(arguments.skipSqlScripts()).isEmpty();
    assertThat(arguments.chunkRows()).isEqualTo(5000);
    assertThat(arguments.mode()).isEqualTo(RunMode.RECOVERY);
  }

  @Test
  public void call_failOnDefiningSqlScriptsAndSkipSqlScripts() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db12.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--sql-scripts",
                "two",
                "--skip-sql-scripts",
                "one"))
        .isEqualTo(2);
    assertThat(writer.toString())
        .contains("The options --sql-scripts and --skip-sql-scripts are mutually exclusive.");
  }

  @Test
  public void call_failOnDefiningEmptySqlScript() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db13.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--sql-scripts",
                "one,,two"))
        .isEqualTo(2);
    assertThat(writer.toString()).contains("SQL script names must not be blank.");
  }

  @Test
  public void call_failOnDefiningEmptySkipSqlScript() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db14.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--skip-sql-scripts",
                "one,,two"))
        .isEqualTo(2);
    assertThat(writer.toString()).contains("SQL script names must not be blank.");
  }

  @Test
  public void call_failOnMissingDbAddress() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString()))
        .isEqualTo(2);
    assertThat(writer.toString()).contains("Missing required option: '--db-address=<dbAddress>'");
  }

  @Test
  public void call_failOnIncorrectOutputPath() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db15.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                "/does/not/exist"))
        .isEqualTo(2);
    assertThat(writer.toString())
        .contains("--output must specify a directory, but '/does/not/exist' is not a directory.");
  }

  @Test
  public void call_failOnIncorrectOutputZipPath() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db16.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                "/does/not/exist/out.zip"))
        .isEqualTo(2);
    assertThat(writer.toString())
        .contains("Parent path of --output '/does/not/exist' is not a directory.");
  }

  @Test
  public void call_failOnIncrementalModeWithoutPrevRunPath() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-inc-fail-no-path.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--run-mode",
                "INCREMENTAL",
                "--rows-per-chunk",
                "5000"))
        .isEqualTo(2);
    assertThat(writer.toString())
        .contains("--run-mode is not NORMAL but --prev-run-path is unspecified.");
  }

  @Test
  public void call_failOnIncrementalModeWithoutChunking() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-inc-fail-no-chunking.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--run-mode",
                "INCREMENTAL",
                "--prev-run-path",
                prevRunPath.toString()))
        .isEqualTo(2);
    assertThat(writer.toString()).contains("Non-normal run modes require chunked processing.");
  }

  @Test
  public void call_failOnIncrementalModeWithZip() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-inc-fail-zip.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--run-mode",
                "INCREMENTAL",
                "--rows-per-chunk",
                "5000",
                "--prev-run-path",
                "/path/ending/with.zip"))
        .isEqualTo(2);
    assertThat(writer.toString())
        .contains("Incremental mode is not supported for zipped records, yet.");
  }

  @Test
  public void call_failOnRecoveryModeWithZip() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-rec-fail-zip.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                "/path/ending/with.zip",
                "--run-mode",
                "RECOVERY",
                "--rows-per-chunk",
                "5000"))
        .isEqualTo(2);
    assertThat(writer.toString())
        .contains("Recovery mode is not supported for zipped records, yet.");
  }

  @Test
  public void call_incrementalModeFailOnIncorrectPrevRunPath() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-inc-fail-wrong-path.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--run-mode",
                "INCREMENTAL",
                "--rows-per-chunk",
                "5000",
                "--prev-run-path",
                "/does/not/exist"))
        .isEqualTo(2);
    assertThat(writer.toString())
        .contains(
            String.format(
                "--prev-run-path must specify a directory, but '%s' is not a directory.",
                "/does/not/exist"));
  }

  @Test
  public void call_recoveryModeFailOnIncorrectPrevRunPath() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:db-rec-fail-wrong-path.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--run-mode",
                "RECOVERY",
                "--rows-per-chunk",
                "5000",
                "--prev-run-path",
                "/does/not/exist"))
        .isEqualTo(2);
    assertThat(writer.toString())
        .contains(
            String.format("The path '%s' you specified is not a directory.", "/does/not/exist"));
  }

  @Test
  public void call_failOnInvalidJdbcUrl() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:fake:doesNotExist",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString()))
        .isEqualTo(2);
    assertThat(writer.toString()).contains("Unable to connect to 'jdbc:fake:doesNotExist'");
  }

  @Test
  public void call_failOnUnknownScriptForScriptBaseDatabase() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db17.example",
                "--db-user",
                "my-username",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString(),
                "--script-base-db",
                "foo=bar"))
        .isEqualTo(2);
    assertThat(writer.toString()).contains("unknown script(s): foo");
  }

  @Test
  public void call_failOnUsernameNotProvided() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db18.example",
                "--db-password",
                "my0password",
                "--output",
                outputPath.toString()))
        .isEqualTo(2);
    assertThat(writer.toString()).contains("Missing required option: '--db-user=<dbUserName>'");
  }

  @Test
  public void call_failOnPasswordNotProvided() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db19.example",
                "--db-user",
                "my-username",
                "--output",
                outputPath.toString()))
        .isEqualTo(2);
    assertThat(writer.toString()).contains("Missing required option: '--db-password'");
  }
}
