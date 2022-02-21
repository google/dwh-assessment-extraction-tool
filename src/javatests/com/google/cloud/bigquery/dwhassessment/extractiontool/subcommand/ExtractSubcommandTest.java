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
import com.google.common.collect.ImmutableMap;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
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

  private final ScriptManager scriptManager =
      new ScriptManagerImpl(
          new ScriptRunnerImpl(),
          ImmutableMap.of("one", () -> "", "two", () -> "", "querylogs", () -> ""),
          ImmutableMap.of());

  @BeforeClass
  public static void setUpClass() throws IOException {
    outputPath = Files.createTempDirectory("extract-test");
  }

  @Test
  public void call_success() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db1.example",
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
  }

  @Test
  public void call_db_user_and_db_password_success() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));

    assertThat(
        cmd.execute(
            "--db-address",
            "jdbc:hsqldb:mem:shiny-brand-new-db",
            "--db-user",
            "dbc",
            "--db-password",
            "dbc",
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
  }

  @Test
  public void call_successWithChunkRows() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db2.example",
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
  public void call_successWithSkipSqlScripts() throws IOException, SQLException {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db4.example",
                "--output",
                outputPath.toString(),
                "--skip-sql-scripts",
                "one,two, three"))
        .isEqualTo(0);

    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);
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

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db5.example",
                "--output",
                outputPath.toString(),
                "--schema-filter",
                "db:foo"))
        .isEqualTo(0);

    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);
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

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db6.example",
                "--output",
                outputPath.toString(),
                "--qrylog-timerange-start",
                "2021-01-01T11:46:46.42134212"))
        .isEqualTo(0);

    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);
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

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db7.example",
                "--output",
                outputPath.toString(),
                "--qrylog-timerange-start",
                "2021-01-01"))
        .isEqualTo(0);

    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);
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

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db8.example",
                "--output",
                outputPath.toString(),
                "--qrylog-timerange-end",
                "2021-01-01T11:46:46.42134212",
                "--time-zone",
                "Europe/Warsaw"))
        .isEqualTo(0);

    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);
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

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db9.example",
                "--output",
                outputPath.toString(),
                "--qrylog-timerange-start",
                "2021-01-01T11:46:46.42134212",
                "--qrylog-timerange-end",
                "2022-01-01",
                "--time-zone",
                "Europe/Warsaw"))
        .isEqualTo(0);

    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);
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

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db10.example",
                "--output",
                outputPath.toString(),
                "--qrylog-timerange-start",
                "2022-01-01T11:46:46.42134212",
                "--qrylog-timerange-end",
                "2021-01-01T11:46:46.42134212",
                "--time-zone",
                "Europe/Warsaw"))
        .isEqualTo(0);

    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);
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

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-db11.example",
                "--output",
                outputPath.toString(),
                "--script-base-db",
                "querylogs=Foo"))
        .isEqualTo(0);

    ArgumentCaptor<ExtractExecutor.Arguments> argumentsCaptor =
        ArgumentCaptor.forClass(ExtractExecutor.Arguments.class);
    verify(executor).run(argumentsCaptor.capture());
    ExtractExecutor.Arguments arguments = argumentsCaptor.getValue();

    assertThat(arguments.scriptBaseDatabase()).containsExactly("querylogs", "Foo");
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

    assertThat(cmd.execute("--output", outputPath.toString())).isEqualTo(2);
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
                "--output",
                "/does/not/exist/out.zip"))
        .isEqualTo(2);
    assertThat(writer.toString())
        .contains("Parent path of --output '/does/not/exist' is not a directory.");
  }

  @Test
  public void call_failOnInvalidJdbcUrl() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor, scriptManager));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address", "jdbc:fake:doesNotExist", "--output", outputPath.toString()))
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
                "--db-address", "jdbc:hsqldb:mem:my-db17.example",
                "--output", outputPath.toString(),
                "--script-base-db", "foo=bar"))
        .isEqualTo(2);
    assertThat(writer.toString()).contains("unknown script(s): foo");
  }
}
