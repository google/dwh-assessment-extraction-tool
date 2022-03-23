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

/** Functional test data generation class. */
public class FunctionalTestDataGenerator {

  public static void main(String[] args) throws SQLException, InterruptedException {
    Connection connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);

    // Generate test data for users.sql
    FunctionalTestDataHelper.generateUsers(connection, 10);

    // Generate test data for columns.sql
    // Generate test data for diskspace.sql
    // Generate test data for indices.sql
    // Generate test data for tableinfo.sql
    // Generate test data for tablesize.sql
    FunctionalTestDataHelper.generateDbTablePairs(connection, 10);

    // Generate test data for functioninfo.sql
    FunctionalTestDataHelper.generateFunctions(connection, 10);

    // Generate test data for all_ri_children.sql
    // Generate test data for all_ri_parents.sql
    FunctionalTestDataHelper.generateConstraints(connection, 10);

    // Generate test data for partitioning_constraints.sql
    FunctionalTestDataHelper.generatePartitioningConstraints(connection, 10);

    // Generate test data for stats.sql
    FunctionalTestDataHelper.generateStats(connection, 10);

    // Generate test data for roles.sql
    FunctionalTestDataHelper.generateRoles(connection, 10);

    // Generate test data for query_references.sql
    FunctionalTestDataHelper.generateQueryReferences(connection, 10);
  }
}
