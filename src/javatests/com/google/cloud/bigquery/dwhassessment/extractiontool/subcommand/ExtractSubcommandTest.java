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
import static org.mockito.Mockito.verify;

import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilter;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilters;
import com.google.cloud.bigquery.dwhassessment.extractiontool.executor.ExtractExecutor;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import picocli.CommandLine;

@RunWith(JUnit4.class)
public final class ExtractSubcommandTest {

  private static Path outputPath;

  @BeforeClass
  public static void setUpClass() throws IOException {
    outputPath = Files.createTempDirectory("extract-test");
  }

  @Test
  public void call_success() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-animalclinic.example",
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
  }

  @Test
  public void call_successWithSqlScripts() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-animalclinic.example",
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
  public void call_successWithSkipSqlScripts() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-animalclinic.example",
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
  public void call_successWithSchemaFilters() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd =
        new CommandLine(new ExtractSubcommand(() -> executor))
            .registerConverter(SchemaFilter.class, SchemaFilters::parse);

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-animalclinic.example",
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
  public void call_failOnDefiningSqlScriptsAndSkipSqlScripts() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-animalclinic.example",
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
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-animalclinic.example",
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
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-animalclinic.example",
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
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(cmd.execute("--output", outputPath.toString())).isEqualTo(2);
    assertThat(writer.toString()).contains("Missing required option: '--db-address=<dbAddress>'");
  }

  @Test
  public void call_failOnIncorrectOutputPath() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address",
                "jdbc:hsqldb:mem:my-animalclinic.example",
                "--output",
                "/does/not/exist"))
        .isEqualTo(2);
    assertThat(writer.toString())
        .contains("--output must specify a directory, but '/does/not/exist' is not a directory.");
  }

  @Test
  public void call_failOnInvalidJdbcUrl() {
    ExtractExecutor executor = Mockito.mock(ExtractExecutor.class);
    CommandLine cmd = new CommandLine(new ExtractSubcommand(() -> executor));
    StringWriter writer = new StringWriter();
    cmd.setErr(new PrintWriter(writer));

    assertThat(
            cmd.execute(
                "--db-address", "jdbc:fake:doesNotExist", "--output", outputPath.toString()))
        .isEqualTo(2);
    assertThat(writer.toString()).contains("Unable to connect to 'jdbc:fake:doesNotExist'");
  }
}
