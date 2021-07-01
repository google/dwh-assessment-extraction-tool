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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Implementation of script manager. Manages mapping from script name to SQL script. Executes script
 * and writes results to an output stream.
 */
public class ScriptManagerImpl implements ScriptManager {

  private static final Logger logger = Logger.getLogger(ScriptManagerImpl.class.getName());

  private final ImmutableMap<String, Supplier<String>> scriptsMap;
  private final ScriptRunner scriptRunner;

  public ScriptManagerImpl(ScriptRunner scriptRunner,
      ImmutableMap<String, Supplier<String>> scriptsMap) {
    this.scriptRunner = scriptRunner;
    this.scriptsMap = scriptsMap;
  }

  @Override
  public void executeScript(
      Connection connection, String scriptName, DataEntityManager dataEntityManager)
      throws SQLException, IOException {
    Preconditions.checkArgument(
        scriptsMap.containsKey(scriptName),
        String.format("Script name %s is not available.", scriptName));
    /* TODO(xshang): figure out how to set schema name and namespace in the schema extraction. */
    String script = scriptsMap.get(scriptName).get();
    Schema schema =
        scriptRunner.extractSchema(
            connection,
            script,
            /*schemaName= */ scriptName,
            /*namespace= */ "namespace");
    ImmutableList<GenericRecord> records =
        scriptRunner.executeScriptToAvro(connection, script, schema);
    dumpResults(records, dataEntityManager.getEntityOutputStream(scriptName), schema);
  }

  @Override
  public ImmutableSet<String> getAllScriptNames() {
    return scriptsMap.keySet();
  }

  private void dumpResults(
      ImmutableList<GenericRecord> records, OutputStream outputStream, Schema schema)
      throws IOException {
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    records.stream()
        .forEach(
            record -> {
              try {
                writer.write(record, encoder);
              } catch (IOException e) {
                throw new IllegalStateException(
                    String.format(
                        "Failed to encode query result %s to file with error message: %s",
                        record.toString(), e.getMessage()));
              }
            });
    encoder.flush();
  }
}
