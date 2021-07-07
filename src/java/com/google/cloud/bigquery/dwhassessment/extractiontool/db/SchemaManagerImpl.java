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

import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManager;
import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/** Implementation of SchemaManager interface to manage database schemas. */
public class SchemaManagerImpl implements SchemaManager {

  @Override
  public void retrieveSchema(
      Connection connection, SchemaKey schemaKey, DataEntityManager dataEntityManager) {
    throw new UnsupportedOperationException("This method will be implemented in following CLs.");
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
          for (SchemaFilter filter : filters) {
            boolean databaseNameMatch;
            if (filter.databaseName().isPresent()) {
              databaseNameMatch = filter.databaseName().get().matcher(databaseName).matches();
            } else {
              databaseNameMatch = true;
            }

            boolean tableNameMatch;
            if (filter.tableName().isPresent()) {
              tableNameMatch = filter.tableName().get().matcher(tableName).matches();
            } else {
              tableNameMatch = true;
            }
            if (databaseNameMatch && tableNameMatch) {
              schemaKeys.add(SchemaKey.create(databaseName, tableName));
            }
          }
        }
      }
    } catch (SQLException exception) {
      throw new InternalError("SQLException while reading data. Reason: %s", exception);
    }
    return schemaKeys.build();
  }
}
