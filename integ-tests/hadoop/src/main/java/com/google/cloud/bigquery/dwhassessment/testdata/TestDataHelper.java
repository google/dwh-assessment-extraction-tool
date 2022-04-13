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
package com.google.cloud.bigquery.dwhassessment.testdata;

import static com.google.cloud.bigquery.dwhassessment.base.Constants.DB_SCHEMAS_FILE;
import static com.google.cloud.bigquery.dwhassessment.base.Constants.SCHEMAS_PATH;
import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static java.lang.System.nanoTime;

import com.google.cloud.bigquery.dwhassessment.base.Constants;
import com.google.cloud.bigquery.dwhassessment.sql.SqlHelper;
import com.google.common.base.Joiner;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class providing test data generation methods */
public final class TestDataHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestDataHelper.class);

  private TestDataHelper() {}

  /**
   * Generates database with tables
   *
   * @param connection connection DB connection parameter
   * @param tableCount tables number to generate
   */
  public static void generateDbWithTables(Connection connection, int tableCount)
      throws SQLException, IOException {
    String createDb = SqlHelper.getSql(Constants.SQL_TEST_DATA_BASE_PATH + "create_db.sql");
    String createTable = SqlHelper.getSql(Constants.SQL_TEST_DATA_BASE_PATH + "create_table.sql");

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

    String schemas = readFromSchemasFile();
    if (schemas.isEmpty()) {
      writeToSchemaFile(dbName);
    } else {
      writeToSchemaFile(String.format("%s,%s", schemas, dbName));
    }
  }

  /**
   * @return String with schemas
   */
  public static String readFromSchemasFile() throws IOException {
    String txt = "";
    File schemaFile = new File(SCHEMAS_PATH + DB_SCHEMAS_FILE);
    if (schemaFile.exists()) {
      try {
        txt = Files.lines(Paths.get(SCHEMAS_PATH + DB_SCHEMAS_FILE)).findFirst().orElse("");
      } catch (IOException e) {
        LOGGER.error("Cannot read schema file");
        throw e;
      }
    }
    return txt;
  }

  private static void writeToSchemaFile(String text) throws IOException {
    try (BufferedWriter bufferedWriter =
        Files.newBufferedWriter(Path.of(SCHEMAS_PATH + DB_SCHEMAS_FILE))) {
      bufferedWriter.write(text);
      bufferedWriter.newLine();
    } catch (IOException e) {
      LOGGER.error("Cannot save schemas to file");
      throw e;
    }
  }

  private static String getRandomDbName() {
    return format("test_db_%s", nanoTime());
  }

  private static String getRandomTableName() {
    return format("test_table_%s", nanoTime());
  }
}
