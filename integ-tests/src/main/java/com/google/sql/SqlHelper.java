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
package com.google.sql;

import static com.google.base.TestConstants.URL_DB;
import static com.google.tdjdbc.JdbcHelper.getIntNotNull;
import static java.lang.String.format;
import static org.apache.commons.io.FileUtils.readFileToString;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class for reading .sql files.
 */
public final class SqlHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlHelper.class);

  private SqlHelper() {
  }

  /**
   * @param sqlPath Path to an .sql file.
   * @return File contents, never null.
   */
  public static String getSql(String sqlPath) {
    try {
      return readFileToString(new File(sqlPath), StandardCharsets.UTF_8);
    } catch (IOException exception) {
      throw new IllegalStateException(
          format("Error while reading sql file %s", sqlPath), exception);
    }
  }

  /**
   * @param connection DB connection parameter
   * @param queries List of strings each of the contains a parametrized SQL request
   */
  public static void executeQueries(Connection connection, List<String> queries)
      throws SQLException {
    for (String query : queries) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
        preparedStatement.execute();
      } catch (SQLException e) {
        LOGGER.error(
            format("Cannot execute query: %n%s%n", query));
        throw e;
      }
    }
  }

  /**
   * @param username DB username
   * @param password DB password
   * @param query A single string of a parametrized SQL request
   */
  public static void connectAndExecuteQueryAsUser(String username, String password, String query)
      throws SQLException {
    Connection connection = DriverManager.getConnection(URL_DB, username, password);
    try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
      preparedStatement.execute();
    } catch (SQLException e) {
      LOGGER.error(
          format("Cannot execute query: %n%s%n", query));
      throw e;
    }
  }

  /**
   * Returns count of rows for provided query.
   *
   * @param connection DB connection parameter
   * @param query Select query with count entries names as RowsNR
   */
  public static int getEntriesCountForQuery(Connection connection, String query)
      throws SQLException {
    int count = 0;
    try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
      ResultSet rs = preparedStatement.executeQuery();
      rs.next();
      count = getIntNotNull(rs, "RowsNR");
    } catch (SQLException e) {
      LOGGER.error(
          format("Cannot execute query: %n%s%n", query));
      throw e;
    }
    return count;
  }

  /**
   * @param connection DB connection parameter
   * @param query Select query for count entries
   * @param max_min Maximum wait time in minutes
   * @param updatedRowsCount Expected count of rows
   */
  public static void waitForRowsUpdate(Connection connection, String query, int max_min,
      int updatedRowsCount) throws InterruptedException, SQLException {
    final Duration TIME_INTERVAL = Duration.ofSeconds(30);
    final Duration MAX_TIME = Duration.ofMinutes(max_min);
    for (long waitTime = 0; waitTime <= MAX_TIME.getSeconds();
        waitTime += TIME_INTERVAL.getSeconds()) {
      if (getEntriesCountForQuery(connection, query) >= updatedRowsCount) {
        break;
      }
      Thread.sleep(TIME_INTERVAL.toMillis());
    }
  }

}
