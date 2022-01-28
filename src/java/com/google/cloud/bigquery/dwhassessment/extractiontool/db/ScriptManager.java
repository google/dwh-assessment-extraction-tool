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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/** Interface to manage SQL scripts. */
public interface ScriptManager {

  /**
   * Executes a script against a DB connection and writes the output to a stream.
   *
   * @param connection The JDBC connection to the database.
   * @param dryRun Whether to just perform a dry run, which just logs out the action to perform.
   * @param sqlTemplateRenderer A transformer to apply on the SQL script before execution.
   * @param scriptName The name of the script. This is not a file name. The interpretation of the
   *     name is left to the implementation but can also map to several files (e.g., an SQL file and
   *     a schema definition file).
   * @param dataEntityManager The data entity manager to use to write the output.
   * @param chunkRows The maximum number of rows (records) in one output file.
   */
  void executeScript(
      Connection connection,
      boolean dryRun,
      SqlTemplateRenderer sqlTemplateRenderer,
      String scriptName,
      DataEntityManager dataEntityManager,
      Integer chunkRows)
      throws SQLException, IOException;

  default void executeScript(
      Connection connection,
      SqlTemplateRenderer sqlTemplateRenderer,
      String scriptName,
      DataEntityManager dataEntityManager)
      throws SQLException, IOException {
    executeScript(
        connection,
        /* dryRun= */ false,
        /* sqlTransformer= */ sqlTemplateRenderer,
        scriptName,
        dataEntityManager,
        0);
  }

  /** Gets a list of names of all available scripts. */
  ImmutableSet<String> getAllScriptNames();

  /** Gets a SQL script based on a script name. */
  String getScript(
      SqlTemplateRenderer sqlTemplateRenderer,
      String scriptName,
      ImmutableList<String> sortingColumns);
}
