package com.google.testdata;

import com.google.base.TestBase;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Test data generation class */
public class TestDataGenerator extends TestBase {

  public static void main(String[] args) throws SQLException {
    Connection connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);

    // Generate test data for users.sql
    TestDataHelper.generateUsers(connection, 10);
  }
}
