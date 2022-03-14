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

import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.AvroHelper.dumpResults;
import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.AvroHelper.getAvroSchema;
import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.cloud.bigquery.dwhassessment.extractiontool.common.ChunkCheckpoint;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilter;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaManager.SchemaKey;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SqlScriptVariables;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SqlScriptVariables.QueryLogsVariables;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SqlTemplateRenderer;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SqlTemplateRendererImpl;
import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/** Default implementation of the extract executor. */
public final class ExtractExecutorImpl implements ExtractExecutor {

  private static final DateTimeFormatter TERADATA_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]xxx").withZone(ZoneOffset.UTC);

  private static final Logger LOGGER = Logger.getLogger(ExtractExecutorImpl.class.getName());

  private final SchemaManager schemaManager;
  private final ScriptManager scriptManager;
  private final SaveChecker saveChecker;
  private final Function<Path, DataEntityManager> dataEntityManagerFactory;

  public ExtractExecutorImpl(
      SchemaManager schemaManager,
      ScriptManager scriptManager,
      SaveChecker saveChecker,
      Function<Path, DataEntityManager> dataEntityManagerFactory) {
    this.scriptManager = scriptManager;
    this.dataEntityManagerFactory = dataEntityManagerFactory;
    this.schemaManager = schemaManager;
    this.saveChecker = saveChecker;
  }

  private static void validateScriptNames(
      String scriptListName, ImmutableSet<String> allNames, ImmutableList<String> input) {
    ImmutableList<String> unknownNames =
        input.stream().filter(name -> !allNames.contains(name)).collect(toImmutableList());
    Preconditions.checkState(
        unknownNames.isEmpty(),
        "Got unknown SQL scripts for %s: %s",
        scriptListName,
        Joiner.on(", ").join(unknownNames));
  }

  @VisibleForTesting
  static String getTeradataTimestampFromInstant(Instant instant) {
    return TERADATA_TIME_FORMATTER.format(instant);
  }

  private static void maybeAddTimeRange(
      SqlScriptVariables.QueryLogsVariables.Builder builder,
      Arguments arguments,
      ChunkCheckpoint checkpoint) {
    // Do not set timeRange on queryLogsVariables if no time specification is required.
    if (!arguments.qryLogStartTime().isPresent()
        && !arguments.qryLogEndTime().isPresent()
        && checkpoint == null) {
      return;
    }
    SqlScriptVariables.QueryLogsVariables.TimeRange.Builder timeRangeBuilder =
        SqlScriptVariables.QueryLogsVariables.TimeRange.builder();
    // Because both incremental and recovery runs assume that the user-specified timeranges do not
    // change between runs, the checkpoint time, if present, overwrites the user-specified start
    // time.
    if (checkpoint != null) {
      timeRangeBuilder.setStartTimestamp(
          getTeradataTimestampFromInstant(checkpoint.lastSavedInstant().plusNanos(1000)));
    } else {
      arguments
          .qryLogStartTime()
          .ifPresent(
              instant ->
                  timeRangeBuilder.setStartTimestamp(getTeradataTimestampFromInstant(instant)));
    }
    arguments
        .qryLogEndTime()
        .ifPresent(
            instant -> timeRangeBuilder.setEndTimestamp(getTeradataTimestampFromInstant(instant)));
    builder.setTimeRange(timeRangeBuilder.build());
  }

  @Override
  public int run(Arguments arguments) throws SQLException, IOException {
    DataEntityManager dataEntityManager = dataEntityManagerFactory.apply(arguments.outputPath());
    ImmutableMap<String, ChunkCheckpoint> checkpoints =
        arguments.mode().equals(RunMode.NORMAL) || arguments.chunkRows() < 1
            ? ImmutableMap.of()
            : arguments
                .prevRunPath()
                .map(saveChecker::getScriptCheckPoints)
                .orElse(ImmutableMap.of());
    SqlScriptVariables.QueryLogsVariables.Builder qryLogVarsBuilder =
        SqlScriptVariables.QueryLogsVariables.builder();
    for (String scriptName : getScriptNames(arguments)) {
      LOGGER.log(Level.INFO, "Start extracting {0}...", scriptName);
      ChunkCheckpoint checkpoint = checkpoints.getOrDefault(scriptName, null);
      maybeAddTimeRange(qryLogVarsBuilder, arguments, checkpoint);
      Connection connection =
          DriverManager.getConnection(
              arguments.dbConnectionAddress(), arguments.dbConnectionProperties());
      SqlTemplateRenderer sqlTemplateRenderer =
          getSqlTemplateRenderer(scriptName, arguments, qryLogVarsBuilder);
      scriptManager.executeScript(
          connection,
          arguments.dryRun(),
          sqlTemplateRenderer,
          scriptName,
          dataEntityManager,
          arguments.chunkRows(),
          checkpoint == null ? 0 : checkpoint.lastSavedChunkNumber() + 1);
      connection.close();
      LOGGER.log(Level.INFO, "Finished extracting {0}.", scriptName);
    }

    if (arguments.dryRun()) {
      LOGGER.log(Level.INFO, "Skipping extracting schemas because dry run was requested.");
    } else {
      LOGGER.log(Level.INFO, "Start extracting schemas");
      try (Connection connection =
          DriverManager.getConnection(
              arguments.dbConnectionAddress(), arguments.dbConnectionProperties())) {
        extractSchema(arguments.schemaFilters(), dataEntityManager, connection);
        LOGGER.log(Level.INFO, "Finish extracting schemas");
      } catch (RuntimeException | SQLException | IOException e) {
        LOGGER.log(Level.WARNING, "Encountered an error while extracting schemas", e);
      }
    }

    dataEntityManager.close();
    return 0;
  }

  private SqlTemplateRenderer getSqlTemplateRenderer(
      String scriptName, Arguments arguments, QueryLogsVariables.Builder qryLogVarsBuilder) {
    SqlScriptVariables.Builder sqlScriptVariablesBuilder =
        SqlScriptVariables.builder()
            .setBaseDatabase(
                arguments.scriptBaseDatabase().getOrDefault(scriptName, arguments.baseDatabase()))
            .setQueryLogsVariables(qryLogVarsBuilder.build())
            .setVars(arguments.scriptVariables().getOrDefault(scriptName, ImmutableMap.of()));
    return new SqlTemplateRendererImpl(sqlScriptVariablesBuilder);
  }

  private void extractSchema(
      ImmutableList<SchemaFilter> schemaFilters,
      DataEntityManager dataEntityManager,
      Connection connection)
      throws SQLException, IOException {
    ImmutableSet<SchemaKey> schemaKeys = schemaManager.getSchemaKeys(connection, schemaFilters);
    if (schemaKeys.isEmpty()) {
      return;
    }
    OutputStream outputStream = dataEntityManager.getEntityOutputStream("schema.avro");
    ImmutableList.Builder<GenericRecord> recordBuilder = new ImmutableList.Builder<>();
    Schema schema =
        getAvroSchema(
            "schema",
            "namespace",
            connection
                .getMetaData()
                .getColumns(
                    /*catalog =*/ null,
                    /*schemaPattern =*/ null,
                    /*tableNamePattern =*/ "%",
                    /*columnNamePattern =*/ null)
                .getMetaData());
    for (SchemaKey schemaKey : schemaKeys) {
      recordBuilder.addAll(schemaManager.retrieveSchema(connection, schemaKey, schema));
    }
    dumpResults(recordBuilder.build(), outputStream, schema);
  }

  private ImmutableCollection<String> getScriptNames(Arguments arguments) {
    ImmutableSet<String> allScriptNames = scriptManager.getAllScriptNames();

    validateScriptNames("skip-sql-scripts", allScriptNames, arguments.skipSqlScripts());
    validateScriptNames("sql-scripts", allScriptNames, arguments.sqlScripts());

    return arguments.sqlScripts().isEmpty()
        ? Sets.difference(allScriptNames, ImmutableSet.copyOf(arguments.skipSqlScripts()))
            .immutableCopy()
        : arguments.sqlScripts();
  }
}
