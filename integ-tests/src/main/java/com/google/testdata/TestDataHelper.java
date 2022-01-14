package com.google.testdata;

import static java.lang.String.format;
import static java.lang.System.nanoTime;

import com.google.sql.SqlHelper;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Helper class providing test data generation methods */
public final class TestDataHelper {

  private TestDataHelper() {}

  /**
   * @param connection DB connection parameter
   * @param userCount Repetition count
   * @throws SQLException
   */
  public static void generateUsers(Connection connection, int userCount) throws SQLException {
    final String sqlDataPath = "src/main/java/com/google/sql/data/users_data.sql";

    List<String> sqlQueries = Collections.nCopies(userCount, SqlHelper.getSql(sqlDataPath));
    sqlQueries =
        sqlQueries.stream().map(e -> e = format(e, nanoTime())).collect(Collectors.toList());
    SqlHelper.executeQueries(connection, sqlQueries);
  }
}
