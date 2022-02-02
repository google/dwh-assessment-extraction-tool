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
package com.google.testdata;

import static com.google.base.TestBase.TESTDATA_SQL_PATH;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.sql.SqlHelper.executeQueries;
import static com.google.sql.SqlHelper.getSql;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.UUID.randomUUID;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class providing test data generation methods
 */
public final class TestDataHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestDataHelper.class);

  private TestDataHelper() {
  }

  /**
   * @param connection DB connection parameter
   * @param userCount Repetition count
   */
  public static void generateUsers(Connection connection, int userCount) throws SQLException {
    final String userData = getSql(TESTDATA_SQL_PATH + "users_data.sql");
    final String password = randomUUID().toString();

    ImmutableList<String> usersQueries =
        Stream.generate(memoize(() -> userData))
            .limit(userCount)
            .map(sqlQuery -> format(sqlQuery, nanoTime(), password))
            .collect(toImmutableList());

    executeQueries(connection, usersQueries);

    LOGGER.info(
        format(
            "Generated %s new User(s):\n%s",
            usersQueries.size(), Joiner.on("\n").join(usersQueries).replaceAll(password, "####")));
  }

  /**
   * @param connection DB connection parameter
   * @param dbTablePairsCount Repetition count
   */
  public static void generateDbTablePairs(Connection connection, int dbTablePairsCount)
      throws SQLException {
    final String dbData = getSql(TESTDATA_SQL_PATH + "columns_data_1.sql");
    final String tableData = getSql(TESTDATA_SQL_PATH + "columns_data_2.sql");

    List<String> dbTableQueries = new ArrayList<>();
    while (dbTablePairsCount > 0) {
      String dbName = format("test_%s_%s", nanoTime(), dbTablePairsCount);
      String tableName = format("test_%s_%s", nanoTime(), dbTablePairsCount);

      dbTableQueries.add(format(dbData, dbName));
      dbTableQueries.add(format(tableData, dbName, tableName));
      dbTablePairsCount--;
    }

    executeQueries(connection, dbTableQueries);

    LOGGER.info(
        format(
            "Generated %s new DB and Table pair(s):\n%s",
            dbTableQueries.size() / 2, Joiner.on("\n").join(dbTableQueries)));
  }

  /**
   * @param connection DB connection parameter
   * @param functionCount Repetition count
   */
  public static void generateFunctions(Connection connection, int functionCount)
      throws SQLException {
    final String functioninfoData = getSql(TESTDATA_SQL_PATH + "functioninfo_data.sql");

    ImmutableList<String> functioninfoQueries =
        Stream.generate(memoize(() -> functioninfoData))
            .limit(functionCount)
            .map(sqlQuery -> format(sqlQuery, nanoTime()))
            .collect(toImmutableList());

    executeQueries(connection, functioninfoQueries);

    LOGGER.info(
        format(
            "Generated %s new function(s):\n%s",
            functioninfoQueries.size(), Joiner.on("\n").join(functioninfoQueries)));
  }

  /**
   * @param connection DB connection parameter
   * @param constraintsCount Repetition count
   */
  public static void generateConstraints(Connection connection, int constraintsCount)
      throws SQLException {
    final String allRiChildrenData1 = getSql(TESTDATA_SQL_PATH + "columns_data_1.sql");
    final String allRiChildrenData2 = getSql(TESTDATA_SQL_PATH + "all_ri_children_data_1.sql");
    final String allRiChildrenData3 = getSql(TESTDATA_SQL_PATH + "all_ri_children_data_2.sql");
    final String allRiChildrenData4 = getSql(TESTDATA_SQL_PATH + "all_ri_children_data_3.sql");

    List<String> dbTableQueries = new ArrayList<>();
    while (constraintsCount > 0) {
      String parentDbName = format("test_%s_%s", nanoTime(), constraintsCount);
      String childDbName = format("test_%s_%s", nanoTime(), constraintsCount);

      dbTableQueries.add(format(allRiChildrenData1, parentDbName));
      dbTableQueries.add(format(allRiChildrenData1, childDbName));

      dbTableQueries.add(format(allRiChildrenData2, parentDbName));
      dbTableQueries.add(format(allRiChildrenData3, childDbName));

      dbTableQueries.add(format(allRiChildrenData4, childDbName, parentDbName));
      constraintsCount--;
    }
    executeQueries(connection, dbTableQueries);

    LOGGER.info(
        format(
            "Generated %s new constraint(s):\n%s",
            dbTableQueries.size(), Joiner.on("\n").join(dbTableQueries)));
  }
}
