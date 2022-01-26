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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A helper class for reading .sql files. */
public final class SqlHelper {

  private static final Logger logger = LoggerFactory.getLogger(SqlHelper.class);

  private SqlHelper() {}

  /**
   * @param sqlPath Path to an .sql file.
   * @return File contents, never null.
   */
  public static String getSql(String sqlPath) {
    try {
      return FileUtils.readFileToString(new File(sqlPath), StandardCharsets.UTF_8);
    } catch (IOException exception) {
      throw new IllegalStateException(
          format("Error while reading sql file %s", sqlPath), exception);
    }
  }

  /**
   * @param connection DB connection parameter
   * @param queries List of strings each of the contains a parametrized SQL request
   * @throws SQLException
   */
  public static void executeQueries(Connection connection, List<String> queries)
      throws SQLException {
    for (String query : queries) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
        preparedStatement.execute();
      } catch (SQLException e) {
        logger.error(format("Cannot execute query: \n%s\n", query));
        throw e;
      }
    }
  }
}
