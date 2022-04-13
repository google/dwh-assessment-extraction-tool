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

import static java.lang.String.format;

import com.google.cloud.bigquery.dwhassessment.base.Constants;
import com.google.cloud.bigquery.dwhassessment.base.TestBase;
import com.google.cloud.bigquery.dwhassessment.dumper.DumperUtils;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaTest extends TestBase {

  private static Connection connection;
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaTest.class);
  private static final String GET_TABLES_QUERY =
      Constants.SQL_TEST_DATA_BASE_PATH + "get_tables.sql";
  private static final String GET_SCHEME_QUERY =
      Constants.SQL_TEST_DATA_BASE_PATH + "get_table_schema.sql";
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
    List<Schema> hiveList = new ArrayList<>();
    for (String db : dbTables.keySet()) {
      for (String tbl : dbTables.get(db)) {
        String query = String.format(SqlHelper.getSql(GET_SCHEME_QUERY), db, tbl);
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
          ResultSet rs = preparedStatement.executeQuery();
          Schema.Builder schemaBuilder = Schema.newBuilder();
          schemaBuilder.setName(tbl);
          while (rs.next()) {
            SqlHelper.parseSchemaRow(rs, schemaBuilder);
          }
          Schema buildSchema = schemaBuilder.build();
          hiveList.add(buildSchema);
        } catch (SQLException e) {
          LOGGER.info(format(SQL_ERROR_MESSAGE, query), e);
          throw e;
        }
      }
    }
    return ImmutableSet.<Schema>builder().addAll(hiveList).build();
  }

  private Map<String, Set<String>> getTables(Set<String> dbList) throws SQLException {
    Map<String, Set<String>> dbTables = new HashMap<>();
    for (String db : dbList) {
      String query = String.format(SqlHelper.getSql(GET_TABLES_QUERY), db);
      try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
        ResultSet rs = preparedStatement.executeQuery();
        Set<String> tables = new HashSet<>();
        while (rs.next()) {
          tables.add(rs.getString("tab_name"));
        }
        dbTables.put(db, tables);
      } catch (SQLException e) {
        LOGGER.info(format(SQL_ERROR_MESSAGE, query), e);
        throw e;
      }
    }
    return dbTables;
  }
}
