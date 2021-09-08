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
package com.google.cloud.bigquery.dwhassessment.extractiontool.db;

import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.AvroHelper.parseRowToAvro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/** Implementation of SchemaManager interface to manage database schemas. */
public class SchemaManagerImpl implements SchemaManager {

  @Override
  public ImmutableList<GenericRecord> retrieveSchema(
      Connection connection, SchemaKey schemaKey, Schema schema) {
    ImmutableList.Builder<GenericRecord> recordsBuilder = new ImmutableList.Builder<>();
    try (ResultSet columnResult =
        connection
            .getMetaData()
            .getColumns(
                /*catalog =*/ null,
                /*schemaPattern =*/ null,
                /*tableNamePattern =*/ schemaKey.tableName(),
                /*columnNamePattern =*/ null)) {
      while (columnResult.next()) {
        recordsBuilder.add(parseRowToAvro(columnResult, schema));
      }
      return recordsBuilder.build();
    } catch (SQLException e) {
      throw new InternalError(
          String.format(
              "Exception while retrieving schema: %s.%s : %s",
              schemaKey.databaseName(), schemaKey.tableName(), e.toString()));
    }
  }

  @Override
  public ImmutableSet<SchemaKey> getSchemaKeys(Connection connection, List<SchemaFilter> filters) {
    ImmutableSet.Builder<SchemaKey> schemaKeys = ImmutableSet.builder();
    try {
      DatabaseMetaData metaData = connection.getMetaData();
      String databaseName = metaData.getDatabaseProductName();
      String[] tableTypes = {"TABLE"};
      ResultSet tablesResultSet =
          metaData.getTables(
              /*catalog=*/ null, /*schemaPattern=*/ null, /*tableNamePattern=*/ null, tableTypes);
      while (tablesResultSet.next()) {
        String tableName = tablesResultSet.getString("TABLE_NAME");
        if (filters.isEmpty()) {
          schemaKeys.add(SchemaKey.create(databaseName, tableName));
        } else {
          boolean databaseNameMatch = true;
          boolean tableNameMatch = true;
          for (SchemaFilter filter : filters) {
            if (filter.databaseName().isPresent()) {
              databaseNameMatch =
                  databaseNameMatch && filter.databaseName().get().matcher(databaseName).matches();
            }

            if (filter.tableName().isPresent()) {
              tableNameMatch =
                  tableNameMatch && filter.tableName().get().matcher(tableName).matches();
            }
          }
          if (databaseNameMatch && tableNameMatch) {
            schemaKeys.add(SchemaKey.create(databaseName, tableName));
          }
        }
      }
    } catch (SQLException exception) {
      throw new InternalError("SQLException while reading data. Reason: %s", exception);
    }
    return schemaKeys.build();
  }
}
