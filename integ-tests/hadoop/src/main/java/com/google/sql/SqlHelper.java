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

import static java.lang.String.format;
import static org.apache.commons.io.FileUtils.readFileToString;

import com.google.testdata.TestDataGenerator;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A helper class for reading .sql files. */
public final class SqlHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestDataGenerator.class);

  private SqlHelper() {}

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
      try (Statement stmt = connection.createStatement()) {
        stmt.execute(query);
      } catch (SQLException e) {
        LOGGER.error(String.format("Cannot execute query: %n%s%n", query));
        throw e;
      }
    }
  }
}
