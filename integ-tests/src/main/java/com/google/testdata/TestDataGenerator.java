package com.google.testdata;

import com.google.base.TestBase;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test data generation class */
public class TestDataGenerator extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(TestDataHelper.class);

  public static void main(String[] args) throws SQLException {
    Connection connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);

    // Generate test data for users.sql
    TestDataHelper.generateUsers(connection, 10);

    // Generate test data for columns.sql
    // Generate test data for diskspace.sql
    // Generate test data for indices.sql
    // Generate test data for tableinfo.sql
    // Generate test data for tablesize.sql
    TestDataHelper.generateDbTablePairs(connection, 10);
  }
}
