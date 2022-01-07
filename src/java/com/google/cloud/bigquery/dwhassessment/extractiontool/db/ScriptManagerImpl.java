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

import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.AvroHelper.parseRowToAvro;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Implementation of script manager. Manages mapping from script name to SQL script. Executes script
 * and writes results to an output stream.
 */
public class ScriptManagerImpl implements ScriptManager {

  private static final Logger LOGGER = Logger.getLogger(ScriptManagerImpl.class.getName());

  private final ImmutableMap<String, Supplier<String>> scriptsMap;
  private final ImmutableMap<String, ImmutableList<String>> sortingColumnsMap;
  private final ScriptRunner scriptRunner;

  public ScriptManagerImpl(
      ScriptRunner scriptRunner,
      ImmutableMap<String, Supplier<String>> scriptsMap,
      ImmutableMap<String, ImmutableList<String>> sortingColumnsMap) {
    this.scriptRunner = scriptRunner;
    this.scriptsMap = scriptsMap;
    this.sortingColumnsMap = sortingColumnsMap;
  }

  @Override
  public void executeScript(
      Connection connection,
      boolean dryRun,
      SqlTemplateRenderer sqlTemplateRenderer,
      String scriptName,
      DataEntityManager dataEntityManager,
      Integer chunkRows)
      throws SQLException, IOException {
    boolean chunkMode =
        chunkRows > 0
            && dataEntityManager.isResumable()
            && sortingColumnsMap.containsKey(scriptName);
    ImmutableList<String> sortingColumns =
        chunkMode ? sortingColumnsMap.get(scriptName) : ImmutableList.of();
    String script = getScript(sqlTemplateRenderer, scriptName, sortingColumns);
    if (dryRun) {
      LOGGER.info(String.format("Should execute script '%s':\n%s", scriptName, script));
      return;
    }
    /* TODO(xshang): figure out how to set schema name and namespace in the schema extraction. */
    Schema schema =
        scriptRunner.extractSchema(connection, script, scriptName, /* namespace= */ "namespace");
    if (chunkMode) {
      ResultSet resultSet = connection.createStatement().executeQuery(script);
      if (!resultSet.next()) {
        return;
      }
      Integer chunkNumber = 0;
      String labelColumn = sortingColumns.get(0);
      while (!resultSet.isAfterLast()) {
        executeScriptChunk(
            resultSet, schema, dataEntityManager, chunkRows, labelColumn, scriptName, chunkNumber);
        chunkNumber++;
      }
      return;
    }
    executeScriptOneSwoop(connection, scriptName, script, schema, dataEntityManager);
  }

  private void executeScriptChunk(
      ResultSet resultSet,
      Schema schema,
      DataEntityManager dataEntityManager,
      Integer chunkRows,
      String labelColumn,
      String scriptName,
      Integer chunkNumber)
      throws SQLException, IOException {
    String firstRowStamp = getUtcTimeStringFromTimestamp(resultSet.getTimestamp(labelColumn));
    String tempFileName =
        String.format("%s-%s_%d_temp.avro", scriptName, firstRowStamp, chunkNumber);
    Timestamp latestTimestamp = resultSet.getTimestamp(labelColumn);
    try (ResultSetRecorder<GenericRecord> dumper =
        AvroResultSetRecorder.create(
            schema, dataEntityManager.getEntityOutputStream(tempFileName))) {
      for (int i = 0; i < chunkRows; i++) {
        // Process first, then advance the row.
        latestTimestamp = resultSet.getTimestamp(labelColumn);
        dumper.add(parseRowToAvro(resultSet, schema));
        if (!resultSet.next()) {
          break;
        }
      }
    } catch (IOException | SQLException e) {
      throw e;
    } catch (Exception e) {
      // Cannot happen.
      throw new IllegalStateException("Got unexpected exception.", e);
    }
    String lastRowStamp = getUtcTimeStringFromTimestamp(latestTimestamp);
    Files.move(
        dataEntityManager.getAbsolutePath(tempFileName),
        dataEntityManager.getAbsolutePath(
            String.format(
                "%s-%s-%s_%d.avro", scriptName, firstRowStamp, lastRowStamp, chunkNumber)),
        ATOMIC_MOVE);
  }

  private void executeScriptOneSwoop(
      Connection connection,
      String scriptName,
      String script,
      Schema schema,
      DataEntityManager dataEntityManager)
      throws SQLException, IOException {
    try (ResultSetRecorder<GenericRecord> dumper =
        AvroResultSetRecorder.create(
            schema, dataEntityManager.getEntityOutputStream(scriptName + ".avro"))) {
      scriptRunner.executeScriptToAvro(connection, script, schema, dumper::add);
    } catch (IOException | SQLException e) {
      throw e;
    } catch (Exception e) {
      // Cannot happen.
      throw new IllegalStateException("Got unexpected exception.", e);
    }
  }

  private String getUtcTimeStringFromTimestamp(Timestamp timestamp) {
    Instant instant = timestamp.toInstant();
    // Remove the "Z" (that signifies UTC timezone) and the separators that are not necessary for
    // human interpretation.
    String instantWithoutNanoseconds =
        instant.truncatedTo(ChronoUnit.SECONDS).toString().replaceAll("[Z\\-:]", "").trim();
    // Keep 6 digits of the fractional seconds from nano, which is equivalent to round(nanos / 10^9
    // * 10^6) with left-padded zeros. Prepend with S to separate from seconds.
    String sixDigitNanos =
        String.format((Locale) null, "S%06d", Math.round(timestamp.getNanos() / Math.pow(10, 3)));
    return instantWithoutNanoseconds + sixDigitNanos;
  }

  @Override
  public String getScript(
      SqlTemplateRenderer sqlTemplateRenderer,
      String scriptName,
      ImmutableList<String> sortingColumns) {
    Preconditions.checkArgument(
        scriptsMap.containsKey(scriptName),
        String.format("Script name %s is not available.", scriptName));
    if (!sortingColumns.isEmpty()) {
      sqlTemplateRenderer.getSqlScriptVariablesBuilder().setSortingColumns(sortingColumns);
    }
    String rendered =
        sqlTemplateRenderer.renderTemplate(scriptName, scriptsMap.get(scriptName).get());
    sqlTemplateRenderer.getSqlScriptVariablesBuilder().setSortingColumns(ImmutableList.of());
    return rendered;
  }

  @Override
  public ImmutableSet<String> getAllScriptNames() {
    return scriptsMap.keySet();
  }
}
