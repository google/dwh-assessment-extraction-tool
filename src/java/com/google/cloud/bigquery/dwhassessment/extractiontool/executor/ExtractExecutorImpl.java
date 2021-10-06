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

import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilter;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaManager.SchemaKey;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManager;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.logging.Level;
import java.io.OutputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.Function;
import org.apache.avro.Schema;
import java.util.logging.Logger;
import org.apache.avro.generic.GenericRecord;

/** Default implementation of the extract executor. */
public final class ExtractExecutorImpl implements ExtractExecutor {
  private final static Logger LOGGER = Logger.getLogger(ExtractExecutorImpl.class.getName());

  private final SchemaManager schemaManager;
  private final ScriptManager scriptManager;
  private final Function<Path, DataEntityManager> dataEntityManagerFactory;

  public ExtractExecutorImpl(
      SchemaManager schemaManager,
      ScriptManager scriptManager,
      Function<Path, DataEntityManager> dataEntityManagerFactory) {
    this.scriptManager = scriptManager;
    this.dataEntityManagerFactory = dataEntityManagerFactory;
    this.schemaManager = schemaManager;
  }

  @Override
  public int run(Arguments arguments) throws SQLException, IOException {
    DataEntityManager dataEntityManager = dataEntityManagerFactory.apply(arguments.outputPath());

    for (String scriptName : getScriptNames(arguments)) {
      LOGGER.log(Level.INFO, "Start extracting {0}...", scriptName);
      Connection connection =
          DriverManager.getConnection(
              arguments.dbConnectionAddress(), arguments.dbConnectionProperties());
      scriptManager.executeScript(connection, scriptName, dataEntityManager);
      connection.close();
      LOGGER.log(Level.INFO, "Finished extracting {0}.", scriptName);
    }

    LOGGER.log(Level.INFO, "Start extracting schemas");
    Connection connection =
        DriverManager.getConnection(
            arguments.dbConnectionAddress(), arguments.dbConnectionProperties());
    extractSchema(arguments.schemaFilters(), dataEntityManager, connection);
    LOGGER.log(Level.INFO, "Finish extracting schemas");

    connection.close();
    dataEntityManager.close();
    return 0;
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
    if (arguments.sqlScripts().isEmpty()) {
      if (arguments.skipSqlScripts().isEmpty()) {
        return allScriptNames;
      } else {
        ImmutableList<String> skip =
            validateScriptNames("skip-sql-scripts", allScriptNames, arguments.skipSqlScripts());
        return allScriptNames.stream()
            .filter(name -> !skip.contains(name))
            .collect(toImmutableList());
      }
    } else {
      return validateScriptNames("sql-scripts", allScriptNames, arguments.sqlScripts());
    }
  }

  private static ImmutableList<String> validateScriptNames(
      String scriptListName, ImmutableSet<String> allNames, ImmutableList<String> input) {
    ImmutableList<String> unknownNames =
        input.stream().filter(name -> !allNames.contains(name)).collect(toImmutableList());
    Preconditions.checkState(
        unknownNames.isEmpty(),
        "Got unknown SQL scripts for %s: %s",
        scriptListName,
        Joiner.on(", ").join(unknownNames));
    return input;
  }
}
