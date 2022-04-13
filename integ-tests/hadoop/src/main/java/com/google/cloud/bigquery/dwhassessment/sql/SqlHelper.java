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
package com.google.cloud.bigquery.dwhassessment.sql;

import static java.lang.String.format;
import static org.apache.commons.io.FileUtils.readFileToString;

import com.google.cloud.bigquery.dwhassessment.proto.HiveSchema.Field;
import com.google.cloud.bigquery.dwhassessment.proto.HiveSchema.Schema;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A helper class for reading .sql files. */
public final class SqlHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlHelper.class);
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy");
  private static final String DB_FIELD_PATTERN = "^[a-z]+$";

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

  /**
   * @param rs row read from hive
   * @param schemaBuilder builder of db schema entity
   */
  public static void parseSchemaRow(ResultSet rs, Schema.Builder schemaBuilder)
      throws SQLException {
    final String dataType = "data_type";
    final String colName = rs.getString("col_name").trim();
    switch (colName) {
      case "Database:":
        schemaBuilder.setSchemaName(rs.getString(dataType));
        break;
      case "Table Type:":
        schemaBuilder.setType(rs.getString(dataType).trim());
        break;
      case "CreateTime:":
        ZonedDateTime zonedDateTimeCT =
            ZonedDateTime.parse(rs.getString(dataType), DATE_TIME_FORMATTER);
        schemaBuilder.setCreateTime(zonedDateTimeCT.toInstant().getEpochSecond());
        break;
      case "LastAccessTime:":
        if (!rs.getString(dataType).trim().equals("UNKNOWN")) {
          ZonedDateTime zonedDateTimeLAT =
              ZonedDateTime.parse(rs.getString(dataType), DATE_TIME_FORMATTER);
          schemaBuilder.setLastAccessTime(zonedDateTimeLAT.toInstant().getEpochSecond());
        }
        break;
      case "Owner:":
        schemaBuilder.setOwner(rs.getString(dataType).trim());
        break;
      case "Location:":
        schemaBuilder.setLocation(rs.getString(dataType).trim());
        break;
      default:
        if (!colName.isEmpty() && colName.matches(DB_FIELD_PATTERN)) {
          schemaBuilder.addFields(
              Field.newBuilder().setName(colName).setType(rs.getString(dataType)).build());
        }
    }
  }
}
