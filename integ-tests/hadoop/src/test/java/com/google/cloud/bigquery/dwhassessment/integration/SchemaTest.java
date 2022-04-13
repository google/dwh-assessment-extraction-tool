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
package com.google.cloud.bigquery.dwhassessment.integration;

import static com.google.cloud.bigquery.dwhassessment.base.Constants.SQL_TEST_DATA_BASE_PATH;
import static java.lang.String.format;

import com.google.cloud.bigquery.dwhassessment.base.Constants;
import com.google.cloud.bigquery.dwhassessment.base.TestBase;
import com.google.cloud.bigquery.dwhassessment.dumper.DumperUtils;
import com.google.cloud.bigquery.dwhassessment.proto.HiveSchema.Partition;
import com.google.cloud.bigquery.dwhassessment.proto.HiveSchema.Schema;
import com.google.cloud.bigquery.dwhassessment.sql.SqlHelper;
import com.google.cloud.bigquery.dwhassessment.testdata.TestDataHelper;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaTest extends TestBase {

  private static Connection connection;
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaTest.class);
  private static final String GET_TABLES_QUERY = SQL_TEST_DATA_BASE_PATH + "get_tables.sql";
  private static final String GET_SCHEMA = SQL_TEST_DATA_BASE_PATH + "get_table_schema.sql";
  private static final String GET_PARTITION_FIELDS =
      SQL_TEST_DATA_BASE_PATH + "get_partitions_fields.sql";
  private static final String GET_PARTITION_INFO =
      SQL_TEST_DATA_BASE_PATH + "get_partition_info.sql";
  private static final String SQL_ERROR_MESSAGE = "Cannot execute query %n%s%n";

  @BeforeClass
  public static void beforeClass() throws SQLException {
    connection =
        DriverManager.getConnection(Constants.URL_DB, Constants.USERNAME_DB, Constants.PASSWORD_DB);
  }

  @Test
  public void schemaTest() throws IOException, SQLException {
    ImmutableSet<Schema> dumperSet = DumperUtils.readSchemas();

    Set<String> dbList =
        new HashSet<>(Arrays.asList(TestDataHelper.readFromSchemasFile().split(",")));
    Map<String, Set<String>> dbTables = getTables(dbList);
    ImmutableSet<Schema> hiveSet = getSchemas(dbTables);

    assertSetsAreEqual(hiveSet, dumperSet);
  }

  private ImmutableSet<Schema> getSchemas(Map<String, Set<String>> dbTables) throws SQLException {
    ImmutableSet.Builder<Schema> schemaSetBuilder = ImmutableSet.builder();
    for (String db : dbTables.keySet()) {
      for (String tbl : dbTables.get(db)) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        getSchemaInformation(db, tbl, schemaBuilder);
        getPartitionInformation(db, tbl, schemaBuilder);
        schemaSetBuilder.add(schemaBuilder.build());
      }
    }
    return schemaSetBuilder.build();
  }

  private void getPartitionInformation(String db, String tbl, Schema.Builder schemaBuilder)
      throws SQLException {
    ImmutableSet<String> partitions = getPartitionsFields(db, tbl);
    for (String partition : partitions) {
      Partition.Builder partitionBuilder = Partition.newBuilder();
      partitionBuilder.setName("state=" + partition);
      String getPartitionInfoQuery =
          format(SqlHelper.getSql(GET_PARTITION_INFO), db, tbl, partition);
      try (PreparedStatement preparedStatement =
          connection.prepareStatement(getPartitionInfoQuery)) {
        ResultSet rs = preparedStatement.executeQuery();
        while (rs.next()) {
          SqlHelper.parsePartitionInfoRow(rs, partitionBuilder);
        }
        schemaBuilder.addPartitions(partitionBuilder.build());
      }
    }
  }

  private ImmutableSet<String> getPartitionsFields(String db, String tbl) throws SQLException {
    ImmutableSet.Builder<String> fieldsSetBuilder = ImmutableSet.builder();
    String getPartitionFieldsQuery = format(SqlHelper.getSql(GET_PARTITION_FIELDS), db, tbl);
    try (PreparedStatement preparedStatement =
        connection.prepareStatement(getPartitionFieldsQuery)) {
      ResultSet rs = preparedStatement.executeQuery();
      while (rs.next()) {
        fieldsSetBuilder.add(rs.getString("state"));
      }
    } catch (SQLException e) {
      LOGGER.info(format(SQL_ERROR_MESSAGE, getPartitionFieldsQuery), e);
      throw e;
    }
    return fieldsSetBuilder.build();
  }

  private void getSchemaInformation(String db, String tbl, Schema.Builder schemaBuilder)
      throws SQLException {
    String getSchemaQuery = format(SqlHelper.getSql(GET_SCHEMA), db, tbl);
    try (PreparedStatement preparedStatement = connection.prepareStatement(getSchemaQuery)) {
      ResultSet rs = preparedStatement.executeQuery();
      schemaBuilder.setName(tbl);
      while (rs.next()) {
        SqlHelper.parseSchemaRow(rs, schemaBuilder);
      }
    } catch (SQLException e) {
      LOGGER.info(format(SQL_ERROR_MESSAGE, getSchemaQuery), e);
      throw e;
    }
  }

  private Map<String, Set<String>> getTables(Set<String> dbList) throws SQLException {
    Map<String, Set<String>> dbTables = new HashMap<>();
    for (String db : dbList) {
      String getTablesQuery = format(SqlHelper.getSql(GET_TABLES_QUERY), db);
      try (PreparedStatement preparedStatement = connection.prepareStatement(getTablesQuery)) {
        ResultSet rs = preparedStatement.executeQuery();
        Set<String> tables = new HashSet<>();
        while (rs.next()) {
          tables.add(rs.getString("tab_name"));
        }
        dbTables.put(db, tables);
      } catch (SQLException e) {
        LOGGER.info(format(SQL_ERROR_MESSAGE, getTablesQuery), e);
        throw e;
      }
    }
    return dbTables;
  }
}
