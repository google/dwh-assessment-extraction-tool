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
package com.google.cloud.bigquery.dwhassessment.extractiontool.subcommand;

import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilter;
import com.google.cloud.bigquery.dwhassessment.extractiontool.executor.ExtractExecutor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
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

  @Spec CommandSpec spec;

  private final Supplier<ExtractExecutor> executorSupplier;

  private final ExtractExecutor.Arguments.Builder argumentsBuilder =
      ExtractExecutor.Arguments.builder();

  public ExtractSubcommand(Supplier<ExtractExecutor> executorSupplier) {
    this.executorSupplier = executorSupplier;
  }

  @Option(
      names = "--db-address",
      required = true,
      description = {
        "JDBC address of the Teradata data warehouse.",
        "Example: jdbc:teradata://storage.my-animalclinic.example"
      })
  void setDbAddress(String dbAddress) {
    argumentsBuilder.setDbAddress(dbAddress);
  }

  @Option(
      names = {"--output", "-o"},
      required = true,
      description = "Output path to which to write the extracted information.")
  void setOutputPath(String pathString) {
    Path path = Paths.get(pathString);
    if (!Files.isDirectory(path)) {
      throw new ParameterException(
          spec.commandLine(),
          String.format("--output must specify a directory, but '%s' is not a directory.", path));
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
        "Multiple filters can be defined by repeating the option. Each filter",
        "has to match (i.e. AND logic)."
      })
  void setSchemaFilters(List<SchemaFilter> schemaFilters) {
    argumentsBuilder.setSchemaFilters(schemaFilters);
  }

  private ExtractExecutor.Arguments getValidatedArguments() {
    ExtractExecutor.Arguments arguments = argumentsBuilder.build();
    if (!arguments.sqlScripts().isEmpty() && !arguments.skipSqlScripts().isEmpty()) {
      throw new ParameterException(
          spec.commandLine(),
          "The options --sql-scripts and --skip-sql-scripts are mutually exclusive.");
    }
    return arguments;
  }

  @Override
  public Integer call() {
    return executorSupplier.get().run(getValidatedArguments());
  }
}
