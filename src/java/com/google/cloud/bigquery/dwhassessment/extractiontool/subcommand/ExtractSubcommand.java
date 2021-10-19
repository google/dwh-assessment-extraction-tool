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

import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilter;
import com.google.cloud.bigquery.dwhassessment.extractiontool.executor.ExtractExecutor;
import com.google.common.collect.ImmutableList;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

/** Subclass for the extract action of the extraction tool. */
@Command(name = "td-extract", description = "Subcommand to extract from a Teradata data warehouse")
public final class ExtractSubcommand implements Callable<Integer> {

  private final Supplier<ExtractExecutor> executorSupplier;
  private final ExtractExecutor.Arguments.Builder argumentsBuilder =
      ExtractExecutor.Arguments.builder();
  @Spec CommandSpec spec;

  @Option(
      names = {"-h", "--help"},
      usageHelp = true)
  private boolean help;

  @Option(
      names = "--db-address",
      required = true,
      description = {
        "JDBC address of the Teradata data warehouse.",
        "Example: jdbc:teradata://storage.my-animalclinic.example"
      })
  private String dbAddress;

  @Option(names = "--db-user", description = "The user name for the database.")
  private String dbUserName = "";

  @Option(names = "--db-password", description = "The password for the database.")
  private String dbPassword = "";

  @Option(
      names = "--base-db",
      defaultValue = "DBC",
      description = "The base database from which to extract the metadata.")
  private String baseDatabase;

  public ExtractSubcommand(Supplier<ExtractExecutor> executorSupplier) {
    this.executorSupplier = executorSupplier;
  }

  @Option(
      names = "--dry-run",
      description = {
        "Whether to do a dry run. A dry run just logs the scripts that would be executed."
      })
  private void setDryRun(boolean dryRun) {
    argumentsBuilder.setDryRun(dryRun);
  }

  @Option(
      names = {"--output", "-o"},
      required = true,
      description = {
        "Output path to which to write the extracted information.",
        "The output is written into a ZIP file if the output path ends in '.zip'.",
        "Otherwise, the output path must be an existing directory."
      })
  void setOutputPath(String pathString) {
    Path path = Paths.get(pathString);
    if (path.toString().endsWith(".zip")) {
      if (!Files.isDirectory(path.getParent())) {
        throw new ParameterException(
            spec.commandLine(),
            String.format("Parent path of --output '%s' is not a directory.", path.getParent()));
      }
    } else {
      if (!Files.isDirectory(path)) {
        throw new ParameterException(
            spec.commandLine(),
            String.format("--output must specify a directory, but '%s' is not a directory.", path));
      }
    }
    argumentsBuilder.setOutputPath(path);
  }

  @Option(
      names = "--sql-scripts",
      split = ",\\s*",
      description = {
        "The list of scripts to execute.",
        "By default, all available scripts will be executed."
      })
  void setSqlScripts(List<String> sqlScripts) {
    for (String sqlScript : sqlScripts) {
      if (sqlScript == null || "".equals(sqlScript)) {
        throw new ParameterException(spec.commandLine(), "SQL script names must not be blank.");
      }
    }
    argumentsBuilder.setSqlScripts(sqlScripts);
  }

  @Option(
      names = "--skip-sql-scripts",
      split = ",\\s*",
      description = {
        "The list of scripts to skip.",
        "By default, all available scripts will be executed."
      })
  void setSkipSqlScripts(List<String> skipSqlScripts) {
    for (String skipSqlScript : skipSqlScripts) {
      if (skipSqlScript == null || "".equals(skipSqlScript)) {
        throw new ParameterException(spec.commandLine(), "SQL script names must not be blank.");
      }
    }
    argumentsBuilder.setSkipSqlScripts(skipSqlScripts);
  }

  @Option(
      names = "--schema-filter",
      description = {
        "The schema filter to apply when extracting schemas from the database.",
        "By default, all schemas will be extracted.",
        "Example:",
        "  db:(abc|def),table:public_.*",
        "  Only take schemas from tables in the database abc or def that have",
        "  the prefix public_.",
        "Multiple filters can be defined by repeating the option. At least one",
        "filter has to match (i.e. OR logic)."
      })
  void setSchemaFilters(List<SchemaFilter> schemaFilters) {
    if (schemaFilters == null | schemaFilters.isEmpty()) {
      argumentsBuilder.setSchemaFilters(
          ImmutableList.of(
              SchemaFilter.builder()
                  .setTableName(Pattern.compile(".*"))
                  .setDatabaseName(Pattern.compile(".*"))
                  .build()));
      return;
    }
    argumentsBuilder.setSchemaFilters(schemaFilters);
  }

  private ExtractExecutor.Arguments getValidatedArguments() {
    try {
      DriverManager.getConnection(dbAddress, dbUserName, dbPassword);
    } catch (SQLException e) {
      throw new ParameterException(
          spec.commandLine(),
          String.format("Unable to connect to '%s': %s", dbAddress, e.getMessage()),
          e);
    }
    Properties connectionProperties = new Properties();
    connectionProperties.put("user", dbUserName);
    connectionProperties.put("password", dbPassword);
    argumentsBuilder.setDbConnectionProperties(connectionProperties);
    argumentsBuilder.setDbConnectionAddress(dbAddress);
    argumentsBuilder.setBaseDatabase(baseDatabase);

    ExtractExecutor.Arguments arguments = argumentsBuilder.build();
    if (!arguments.sqlScripts().isEmpty() && !arguments.skipSqlScripts().isEmpty()) {
      throw new ParameterException(
          spec.commandLine(),
          "The options --sql-scripts and --skip-sql-scripts are mutually exclusive.");
    }
    return arguments;
  }

  @Override
  public Integer call() throws IOException, SQLException {
    return executorSupplier.get().run(getValidatedArguments());
  }
}
