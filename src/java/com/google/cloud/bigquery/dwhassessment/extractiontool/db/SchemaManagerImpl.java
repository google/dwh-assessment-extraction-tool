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

import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.AvroHelper.dumpResults;
import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.AvroHelper.parseRowToAvro;

import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/** Implementation of SchemaManager interface to manage database schemas. */
public class SchemaManagerImpl implements SchemaManager {
  // Using a static string instead of using AvroHelper.getAvroSchema() because DECIMAL_DIGITS has
  // type INTEGER but requires type long in AVRO, else it would give
  // "org.apache.avro.UnresolvedUnionException: Not in union [“null”,“int”]" error.
  private static final String SCHEMA_JSON =
      "{\"type\":\"record\","
          + "\"name\":\"%s\","
          + "\"namespace\":\"%s\","
          + "\"fields\":["
          + "{\"name\":\"TABLE_CAT\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"TABLE_SCHEM\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"TABLE_NAME\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"COLUMN_NAME\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"DATA_TYPE\",\"type\":[\"null\",\"int\"],\"default\":null},"
          + "{\"name\":\"TYPE_NAME\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"COLUMN_SIZE\",\"type\":[\"null\",\"int\"],\"default\":null},"
          + "{\"name\":\"BUFFER_LENGTH\",\"type\":[\"null\",\"int\"],\"default\":null},"
          + "{\"name\":\"DECIMAL_DIGITS\",\"type\":[\"null\",\"long\"],\"default\":null},"
          + "{\"name\":\"NUM_PREC_RADIX\",\"type\":[\"null\",\"int\"],\"default\":null},"
          + "{\"name\":\"NULLABLE\",\"type\":[\"null\",\"int\"],\"default\":null},"
          + "{\"name\":\"REMARKS\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"COLUMN_DEF\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"SQL_DATA_TYPE\",\"type\":[\"null\",\"int\"],\"default\":null},"
          + "{\"name\":\"SQL_DATETIME_SUB\",\"type\":[\"null\",\"int\"],\"default\":null},"
          + "{\"name\":\"CHAR_OCTET_LENGTH\",\"type\":[\"null\",\"int\"],\"default\":null},"
          + "{\"name\":\"ORDINAL_POSITION\",\"type\":[\"null\",\"int\"],\"default\":null},"
          + "{\"name\":\"IS_NULLABLE\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"SCOPE_CATALOG\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"SCOPE_SCHEMA\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"SCOPE_TABLE\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"SOURCE_DATA_TYPE\",\"type\":[\"null\",\"int\"],\"default\":null},"
          + "{\"name\":\"IS_AUTOINCREMENT\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"IS_GENERATEDCOLUMN\",\"type\":[\"null\",\"string\"],\"default\":null}]}\n";

  @Override
  public void retrieveSchema(
      Connection connection, SchemaKey schemaKey, DataEntityManager dataEntityManager) {
    ImmutableList.Builder<GenericRecord> recordsBuilder = ImmutableList.builder();
    try {
      try (ResultSet columnResult =
          connection
              .getMetaData()
              .getColumns(
                  /*catalog =*/ null,
                  /*schemaPattern =*/ null,
                  /*tableNamePattern =*/ schemaKey.tableName(),
                  /*columnNamePattern =*/ null)) {
        Schema columnResultSchema =
            new Schema.Parser()
                .parse(String.format(SCHEMA_JSON, schemaKey.tableName(), schemaKey.databaseName()));
        ;
        while (columnResult.next()) {
          recordsBuilder.add(parseRowToAvro(columnResult, columnResultSchema));
        }
        OutputStream outputStream =
            dataEntityManager.getEntityOutputStream(
                schemaKey.databaseName() + "_" + schemaKey.tableName());
        dumpResults(recordsBuilder.build(), outputStream, columnResultSchema);
      }
    } catch (SQLException | IOException e) {
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
