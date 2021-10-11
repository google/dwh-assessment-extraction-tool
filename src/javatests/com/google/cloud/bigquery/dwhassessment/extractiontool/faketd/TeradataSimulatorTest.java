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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TeradataSimulatorTest {

  @Test
  public void createTablesAndViews_success() throws SQLException, IOException {
    TeradataSimulator.createTablesAndViews("jdbc:hsqldb:mem:faketd");

    try (Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:faketd")) {
      try (Statement statement = connection.createStatement()) {
        try (ResultSet rs =
            statement.executeQuery(
                "SELECT"
                    + "  \"DatabaseName\","
                    + "  \"TableName\","
                    + "  \"LastAccessTimeStamp\" AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE "
                    + "FROM DBC.\"TablesV\"")) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void runSqlFile_success() throws SQLException, IOException {
    TeradataSimulator.createTablesAndViews("jdbc:hsqldb:mem:insert");

    try (Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:insert")) {
      TeradataSimulator.runSqlFile("jdbc:hsqldb:mem:insert",
          TeradataSimulatorTest.class.getResource("insert_test.sql"));

      try (Statement statement = connection.createStatement()) {
        try (ResultSet rs = statement.executeQuery("SELECT * FROM DBC.\"TablesV\"")) {
          assertThat(rs.next()).isTrue();
        }
      }
    }
  }

  @Test
  public void runSqlFile_invalidSql_failure() throws SQLException, IOException {
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> TeradataSimulator
        .runSqlFile("jdbc:hsqldb:mem:invalid",
            TeradataSimulatorTest.class.getResource("invalid_test.sql")));
    assertThat(e).hasMessageThat().contains("invalid sql syntax");
  }
}
