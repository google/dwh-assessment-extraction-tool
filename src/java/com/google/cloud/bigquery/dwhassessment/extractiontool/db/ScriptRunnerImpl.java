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

import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.AvroHelper.getAvroSchema;
import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.AvroHelper.parseRowToAvro;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Implementation of ScriptRunner. Executes SQL script and converts query results to desired format,
 * i.e. Avro.
 */
public class ScriptRunnerImpl implements ScriptRunner {

  @Override
  public ImmutableList<GenericRecord> processResultsToAvro(
      ResultSet resultSet, Schema schema, Integer maxRows) throws SQLException {
    ImmutableList.Builder<GenericRecord> recordsBuilder = ImmutableList.builder();
    Integer processedRows = 0;
    boolean isRowLimited = maxRows > 0;
    while (resultSet.next()) {
      recordsBuilder.add(parseRowToAvro(resultSet, schema));
      processedRows++;
      if (isRowLimited && processedRows >= maxRows) {
        break;
      }
    }
    return recordsBuilder.build();
  }

  @Override
  public Schema extractSchema(
      Connection connection, String sqlScript, String schemaName, String namespace)
      throws SQLException {
    ResultSetMetaData metaData = connection.createStatement().executeQuery(sqlScript).getMetaData();
    return getAvroSchema(schemaName, namespace, metaData);
  }
}
