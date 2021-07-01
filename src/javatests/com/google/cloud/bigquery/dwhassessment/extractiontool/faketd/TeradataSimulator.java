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
package com.google.cloud.bigquery.dwhassessment.extractiontool.faketd;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;

/** An in-memory DB using HSQLDB mimicking a Teradata DB. */
public class TeradataSimulator {

  private TeradataSimulator() {}

  /**
   * Populate the given database with DBC tables / views.
   *
   * @param dbUrl The address of the DB to populate.
   */
  public static void populate(String dbUrl) throws SQLException, IOException {
    try (Connection connection = DriverManager.getConnection(dbUrl)) {
      URL url = TeradataSimulator.class.getResource("teradata_tables.sql");
      SqlFile sqlFile = new SqlFile(url);
      sqlFile.setConnection(connection);
      try {
        sqlFile.execute();
      } catch (SqlToolError e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
