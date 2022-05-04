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
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.executor.ExtractExecutor;
import com.google.cloud.bigquery.dwhassessment.extractiontool.executor.ExtractExecutor.RunMode;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.zone.ZoneRulesException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
  private final ScriptManager scriptManager;
  private final ExtractExecutor.Arguments.Builder argumentsBuilder =
      ExtractExecutor.Arguments.builder();

  private Instant getUtcInstantFromDatetimeAndZone(String dateTime, String zoneId) {
    DateTimeFormatter flexibleFormatter =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .optionalStart()
            .appendLiteral('T')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .toFormatter();
    try {
      TemporalAccessor temporalAccessor =
          flexibleFormatter.parseBest(dateTime, LocalDateTime::from, LocalDate::from);
      if (temporalAccessor instanceof LocalDateTime) {
        return ZonedDateTime.of(LocalDateTime.from(temporalAccessor), ZoneId.of(zoneId))
            .toInstant();
      }
      return ZonedDateTime.of(LocalDate.from(temporalAccessor).atStartOfDay(), ZoneId.of(zoneId))
          .toInstant();
    } catch (DateTimeParseException | ZoneRulesException e) {
      throw new ParameterException(
          spec.commandLine(), "Unable to parse time specification: ", e.getCause());
    }
  }

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

  @Option(names = "--db-user", required = true, description = "The user name for the database.")
  private String dbUserName;

  @Option(
      names = "--db-password",
      description = {
        "The password for the database. If --db-password is not followed by a string, you will be",
        " prompted to enter the password interactively, masked."
      },
      arity = "0..1",
      required = true,
      interactive = true)
  private String dbPassword = "";

  @Option(
      names = "--base-db",
      defaultValue = "DBC",
      description = "The base database from which to extract the metadata.")
  private void setBaseDatabase(String baseDatabase) {
    argumentsBuilder.setBaseDatabase(baseDatabase);
  }

  @Option(
      names = "--script-base-db",
      split = ",",
      description = "Overwrite the base database for a specific script.")
  private void scriptBaseDatabase(Map<String, String> scriptBaseDatabase) {
    ImmutableSet<String> allScriptNames = ImmutableSet.copyOf(scriptManager.getAllScriptNames());
    SetView<String> unknownScripts = Sets.difference(scriptBaseDatabase.keySet(), allScriptNames);
    if (!unknownScripts.isEmpty()) {
      throw new ParameterException(
          spec.commandLine(),
          String.format("Got unknown script(s): %s", Joiner.on(", ").join(unknownScripts)));
    }
    argumentsBuilder.setScriptBaseDatabase(ImmutableMap.copyOf(scriptBaseDatabase));
  }

  @Option(
      names = "--script-vars",
      split = ",",
      description = {
        "Provide optional script variables. The variable a for a script b is set to value v",
        "like this: a.b=v"
      })
  private void scriptVars(Map<String, String> vars) {
    Map<String, Map<String, String>> scriptVars = new HashMap<>();
    for (Map.Entry<String, String> entry : vars.entrySet()) {
      String[] parts = entry.getKey().split("\\.", 2);
      if (!scriptVars.containsKey(parts[0])) {
        scriptVars.put(parts[0], new HashMap<>());
      }
      scriptVars.get(parts[0]).put(parts[1], entry.getValue());
    }
    ImmutableSet<String> allScriptNames = ImmutableSet.copyOf(scriptManager.getAllScriptNames());
    SetView<String> unknownScripts = Sets.difference(scriptVars.keySet(), allScriptNames);
    if (!unknownScripts.isEmpty()) {
      throw new ParameterException(
          spec.commandLine(),
          String.format("Got unknown script(s): %s", Joiner.on(", ").join(unknownScripts)));
    }
    argumentsBuilder.setScriptVariables(ImmutableMap.copyOf(scriptVars));
  }

  @Option(
      names = "--time-zone",
      defaultValue = "Z",
      description =
          "The time-zone id for specified timestamps, with format complying with java.time.ZoneID's"
              + " default options"
              + " (https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html#of-java.lang.String-)."
              + " If unspecified, evaluates to time-zone UTC.")
  private String timeZone;

  @Option(
      names = "--qrylog-timerange-start",
      description = {
        "The local start of the time range for the query logs to retrieve.",
        "The format is 'yyyy-mm-dd[Thh:mm:ss[.n...]]'. Example: '2007-12-03T10:15:30.233333'.",
        "If only the date part is specified, then time defaults to 00:00:00.00 of the given date.",
        "If --run-mode is INCREMENTAL, this will be overwritten by the timestamp of the latest"
            + " record.",
        "Note that this also applies to sql_logs script but on a timestamp that is similar but can"
            + " be seconds later than that of querylogs, so you may find extra results from"
            + " sql_logs near the beginning of the time range compared to querylogs."
      })
  private String startTimeString;

  @Option(
      names = "--qrylog-timerange-end",
      description = {
        "The local end of the time range for the query logs to retrieve.",
        "The format is 'yyyy-mm-dd[Thh:mm:ss[.n...]]'. Example: '2007-12-03T10:15:30.233333'.",
        "If only the date part is specified, then time defaults to 00:00:00.00 of the given date.",
        "Note that this also applies to sql_logs script but on a timestamp that is similar but can"
            + " be seconds later than that of querylogs, so some results near the end of the time"
            + " range may be missing from sql_logs."
      })
  private String endTimeString;

  @Option(
      names = "--dry-run",
      description = {
        "Whether to do a dry run. A dry run just logs the scripts that would be executed."
      })
  private void setDryRun(boolean dryRun) {
    argumentsBuilder.setDryRun(dryRun);
  }

  @Option(
      names = "--rows-per-chunk",
      defaultValue = "0",
      description = {
        "If larger than 0, the tool will attempt to use a chunked processing mode for scripts that"
            + " support this, where the results for a supporting script are saved in chunks; this"
            + " number defines the maximum rows per chunk holds. Chunk mode is not available if the"
            + " target output is a zip file."
      })
  private Integer chunkRows;

  @Option(
      names = {"--output", "-o"},
      required = true,
      description = {
        "Output path to which to write the extracted information.",
        "The output is written into a ZIP file if the output path ends in '.zip'.",
        "Otherwise, the output path must be an existing directory."
      })
  private String outputPathString;

  @Option(
      names = "--run-mode",
      description = {
        "Available modes: ${COMPLETION-CANDIDATES}",
        "INCREMENTAL:",
        "  Continue from a previous successful run for script supporting chunked mode.",
        "  Scripts not supporting chunked mode will be run as normal.",
        "  It is the user's duty to ensure that user-specified time range remains the same as the"
            + " previous run(s).",
        "RECOVERY:",
        "  Resume a previously interrupted run.",
        "  For scripts producing non-chunked records, file names corresponding to"
            + " '<scriptname>.avro' are regarded as finished; for scripts producing chunked"
            + " records, will continue in the same way as in INCREMENTAL mode.",
        "  If --prev-run-path is unspecified, will treat the output path as the previous run path.",
        "  Note: Database changes made between the original and recovery runs for the finished"
            + " scripts may not be captured; if the time gap between the two runs is long,"
            + " recommend using INCREMENTAL mode instead.",
      },
      defaultValue = "NORMAL")
  private RunMode mode;

  @Option(
      names = "--need-querytext",
      negatable = true,
      description = {
        "Whether to extract the query texts. Default true.",
        "If false, fields containing query text will not be extracted;",
        "the values for them in the result will all be the string '_'."
      })
  // picocli has a quirky requirement that "when a negatable option is true by default, give it the
  // negative name", otherwise it would not work. So we have to define the default behavior of
  // needQueryText in the actual argument setter.
  private Boolean needQueryText;

  @Option(
      names = {"--prev-run-path"},
      description = {
        "Path containing records of previous run(s).",
        "Can only be a directory. Needs to be specified for incremental / recovery runs."
      })
  private String prevRunPathString;

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
      names = "--qrylog-users",
      split = ",",
      description = {"If specified, only retrieves query logs by users with those UserName(s)."})
  void setFilterUsers(Set<String> users) {
    argumentsBuilder.setQryLogUsers(users);
  }

  @Option(
      names = "--need-jdbc-schemas",
      negatable = true,
      description = "Whether to extract schemas through JDBC. Default: ${DEFAULT-VALUE}")
  private Boolean needJdbcSchemas;

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
    if (schemaFilters == null || schemaFilters.isEmpty()) {
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

  public ExtractSubcommand(
      Supplier<ExtractExecutor> executorSupplier, ScriptManager scriptManager) {
    this.executorSupplier = executorSupplier;
    this.scriptManager = scriptManager;
  }

  private ExtractExecutor.Arguments getValidatedArguments() {
    argumentsBuilder.setNeedQueryText(true).setNeedJdbcSchemas(true);
    if (needQueryText != null) {
      argumentsBuilder.setNeedQueryText(needQueryText);
    }
    if (needJdbcSchemas != null) {
      argumentsBuilder.setNeedJdbcSchemas(needJdbcSchemas);
    }
    switch (mode) {
      case INCREMENTAL:
        validateAndSetPrevRunPathIncrementalMode();
        break;
      case RECOVERY:
        validateAndSetPrevRunPathRecoveryMode();
        break;
      case NORMAL:
        break;
      default:
        throw new ParameterException(spec.commandLine(), "Unknown mode specified.");
    }
    validateAndSetOutputPath();
    argumentsBuilder.setMode(mode).setChunkRows(chunkRows);

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
    argumentsBuilder
        .setDbConnectionProperties(connectionProperties)
        .setDbConnectionAddress(dbAddress);

    if (!Strings.isNullOrEmpty(startTimeString)) {
      argumentsBuilder.setQryLogStartTime(
          getUtcInstantFromDatetimeAndZone(startTimeString, timeZone));
    }
    if (!Strings.isNullOrEmpty(endTimeString)) {
      argumentsBuilder.setQryLogEndTime(getUtcInstantFromDatetimeAndZone(endTimeString, timeZone));
    }

    ExtractExecutor.Arguments arguments = argumentsBuilder.build();
    if (!arguments.sqlScripts().isEmpty() && !arguments.skipSqlScripts().isEmpty()) {
      throw new ParameterException(
          spec.commandLine(),
          "The options --sql-scripts and --skip-sql-scripts are mutually exclusive.");
    }
    return arguments;
  }

  private void validateAndSetOutputPath() {
    Path path = Paths.get(outputPathString);
    if (path.toString().endsWith(".zip") && !Files.isDirectory(path.getParent())) {
      throw new ParameterException(
          spec.commandLine(),
          String.format("Parent path of --output '%s' is not a directory.", path.getParent()));
    } else if (!Files.isDirectory(path)) {
      throw new ParameterException(
          spec.commandLine(),
          String.format("--output must specify a directory, but '%s' is not a directory.", path));
    }
    argumentsBuilder.setOutputPath(path);
  }

  private void validateAndSetPrevRunPathIncrementalMode() {
    if (chunkRows < 1) {
      throw new ParameterException(
          spec.commandLine(),
          "Non-normal run modes require chunked processing. Set --rows-per-chunk to a positive"
              + " integer to enable chunked processing.");
    }
    if (prevRunPathString == null) {
      throw new ParameterException(
          spec.commandLine(), "--run-mode is not NORMAL but --prev-run-path is unspecified.");
    }
    if (prevRunPathString.endsWith(".zip")) {
      throw new ParameterException(
          spec.commandLine(), "Incremental mode is not supported for zipped records, yet.");
    }
    Path path = Paths.get(prevRunPathString);
    if (!Files.isDirectory(path)) {
      throw new ParameterException(
          spec.commandLine(),
          String.format(
              "--prev-run-path must specify a directory, but '%s' is not a directory.", path));
    }
    argumentsBuilder.setPrevRunPath(path);
  }

  private void validateAndSetPrevRunPathRecoveryMode() {
    String pathString = prevRunPathString == null ? outputPathString : prevRunPathString;
    if (pathString.endsWith(".zip")) {
      throw new ParameterException(
          spec.commandLine(), "Recovery mode is not supported for zipped records, yet.");
    }
    Path path = Paths.get(pathString);
    if (!Files.isDirectory(path)) {
      throw new ParameterException(
          spec.commandLine(),
          String.format("The path '%s' you specified is not a directory.", path));
    }
    argumentsBuilder.setPrevRunPath(path);
  }

  @Override
  public Integer call() throws IOException, SQLException {
    return executorSupplier.get().run(getValidatedArguments());
  }
}
