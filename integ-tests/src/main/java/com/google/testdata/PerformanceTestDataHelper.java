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

import static com.google.base.TestConstants.SQL_TESTDATA_BASE_PATH;
import static com.google.sql.SqlUtil.executeQueries;
import static com.google.sql.SqlUtil.getSql;
import static com.google.testdata.TestDataHelper.getRandomDbName;
import static com.google.testdata.TestDataHelper.getRandomTableName;
import static java.lang.String.format;
import static java.lang.System.lineSeparator;

import com.google.common.base.Joiner;
import com.google.testdata.pojo.TestDataUser;
import com.google.testdata.utils.PerformanceTestDataGeneratorQueryExecutor;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class providing test data generation methods */
public final class PerformanceTestDataHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceTestDataHelper.class);

  private PerformanceTestDataHelper() {}

  /**
   * @param connection DB connection parameter
   * @param tablesCount Count of tables to generate
   */
  public static void generateTables(Connection connection, int tablesCount) throws SQLException {
    final int USER_COUNT = 5;
    final int TABLES_PER_USER = tablesCount / USER_COUNT;
    final int SIZE = 2048 * TABLES_PER_USER;

    HashMap<TestDataUser, List<String>> performancePayload = new HashMap<>();
    List<String> preconditionQueries = new ArrayList<>();

    final String createUserSql = getSql(SQL_TESTDATA_BASE_PATH + "users_data.sql");
    final String createDbSql = getSql(SQL_TESTDATA_BASE_PATH + "performance_data_1.sql");
    final String grantCreateTableSql = getSql(SQL_TESTDATA_BASE_PATH + "performance_data_2.sql");
    final String createTableSql = getSql(SQL_TESTDATA_BASE_PATH + "columns_data_2.sql");

    for (int i = 0; i < USER_COUNT; i++) {
      List<String> queries = new ArrayList<>();
      TestDataUser userData = new TestDataUser();
      String dbName = getRandomDbName();

      String createUser = format(createUserSql, userData.getUsername(), userData.getPassword());
      String createDatabase = format(createDbSql, dbName, SIZE, SIZE, SIZE);
      String grantCreateTable = format(grantCreateTableSql, dbName, userData.getUsername());
      preconditionQueries.addAll(Arrays.asList(createUser, createDatabase, grantCreateTable));

      for (int j = 0; j < TABLES_PER_USER; j++) {
        String tableName = getRandomTableName();
        String createTable = format(createTableSql, dbName, tableName);
        queries.add(createTable);
      }
      performancePayload.put(userData, queries);
    }

    executeQueries(connection, preconditionQueries);
    LOGGER.info(
        format(
            "Performance test precondition queries %n%s",
            Joiner.on(lineSeparator()).join(preconditionQueries)));

    PerformanceTestDataGeneratorQueryExecutor performanceTasks =
        new PerformanceTestDataGeneratorQueryExecutor(performancePayload, 0, USER_COUNT);
    new ForkJoinPool().invoke(performanceTasks);
  }
}
