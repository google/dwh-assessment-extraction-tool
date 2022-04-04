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

import static com.google.base.Constants.SQL_TEST_DATA_BASE_PATH;
import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static java.lang.System.nanoTime;

import com.google.common.base.Joiner;
import com.google.sql.SqlHelper;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class providing test data generation methods */
public final class TestDataHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestDataGenerator.class);

  private TestDataHelper() {}

  /**
   * Generates database with tables
   *
   * @param connection connection DB connection parameter
   * @param tableCount tables number to generate
   */
  public static void generateDbWithTables(Connection connection, int tableCount)
      throws SQLException {
    String createDb = SqlHelper.getSql(SQL_TEST_DATA_BASE_PATH + "create_db.sql");
    String createTable = SqlHelper.getSql(SQL_TEST_DATA_BASE_PATH + "create_table.sql");

    String dbName = getRandomDbName();
    List<String> queryList = new ArrayList<>();

    String createDbQuery = String.format(createDb, dbName);
    queryList.add(createDbQuery);
    for (int i = 0; i < tableCount; i++) {
      String tableName = getRandomTableName();
      String createTableQuery = String.format(createTable, dbName, tableName);
      queryList.add(createTableQuery);
    }

    SqlHelper.executeQueries(connection, queryList);

    LOGGER.info(
        String.format(
            "Generated %s new constraints:%n%s",
            queryList.size(), Joiner.on(lineSeparator()).join(queryList)));
  }

  private static String getRandomDbName() {
    return format("test_db_%s", nanoTime());
  }

  private static String getRandomTableName() {
    return format("test_table_%s", nanoTime());
  }
}
