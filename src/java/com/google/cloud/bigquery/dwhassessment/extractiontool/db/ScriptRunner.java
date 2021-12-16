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

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/** Interface to execute SQL scripts. */
public interface ScriptRunner {

  /**
   * Executes a script against a DB connection and writes the output to a stream.
   *
   * @param resultSet The full SQL script to execute.
   * @param schema The schema corresponding to the script. If the schema is missing, run the
   *     extractSchema method first to get the schema of the query.
   * @param maxRows
   * @return a collection of Avro records.
   */
  ImmutableList<GenericRecord> processResultsToAvro(
      ResultSet resultSet, Schema schema, Integer maxRows) throws SQLException;

  /**
   * Extracts the schema based on a SQL query. This method can extract the schema of a table by
   * selecting all columns from the table, e.g. SELECT * FROM TABLE.
   *
   * @param connection The JDBC connection to the database.
   * @param sqlScript The complete SQL script to execute.
   * @param schemaName The name of the output schema.
   * @param namespace The namespace of the output schema.
   * @return the schema in Avro format.
   */
  Schema extractSchema(Connection connection, String sqlScript, String schemaName, String namespace)
      throws SQLException;
}
