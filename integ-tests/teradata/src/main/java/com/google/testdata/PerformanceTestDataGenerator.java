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

import static com.google.base.TestConstants.PASSWORD_DB;
import static com.google.base.TestConstants.URL_DB;
import static com.google.base.TestConstants.USERNAME_DB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Test data generation class for performance tests. */
public class PerformanceTestDataGenerator {

  public static void main(String[] args) throws SQLException {
    Connection connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);

    // Generate test data for indices.sql in multiple threads
    PerformanceTestDataHelper.generateTables(connection, 10);
  }
}
