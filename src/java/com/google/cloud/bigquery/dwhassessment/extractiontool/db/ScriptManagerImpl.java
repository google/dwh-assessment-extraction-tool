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

import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Implementation of script manager. Manages mapping from script name to SQL script. Executes script
 * and writes results to an output stream.
 */
public class ScriptManagerImpl implements ScriptManager {

  private static final Logger LOGGER = Logger.getLogger(ScriptManagerImpl.class.getName());

  private final ImmutableMap<String, Supplier<String>> scriptsMap;
  private final ScriptRunner scriptRunner;

  public ScriptManagerImpl(
      ScriptRunner scriptRunner, ImmutableMap<String, Supplier<String>> scriptsMap) {
    this.scriptRunner = scriptRunner;
    this.scriptsMap = scriptsMap;
  }

  @Override
  public void executeScript(
      Connection connection,
      boolean dryRun,
      SqlTemplateRenderer sqlTemplateRenderer,
      String scriptName,
      DataEntityManager dataEntityManager)
      throws SQLException, IOException {
    String script = getScript(sqlTemplateRenderer, scriptName);
    if (dryRun) {
      LOGGER.info(String.format("Should execute script '%s':\n%s", scriptName, script));
      return;
    }

    /* TODO(xshang): figure out how to set schema name and namespace in the schema extraction. */
    Schema schema =
        scriptRunner.extractSchema(connection, script, scriptName, /* namespace= */ "namespace");
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

  @Override
  public String getScript(SqlTemplateRenderer sqlTemplateRenderer, String scriptName) {
    Preconditions.checkArgument(
        scriptsMap.containsKey(scriptName),
        String.format("Script name %s is not available.", scriptName));
    return sqlTemplateRenderer.renderTemplate(scriptName, scriptsMap.get(scriptName).get());
  }

  @Override
  public ImmutableSet<String> getAllScriptNames() {
    return scriptsMap.keySet();
  }
}
