package com.google.testdata;

import static java.lang.String.format;
import static java.lang.System.nanoTime;

import com.google.base.TestBase;
import com.google.common.base.Joiner;
import com.google.sql.SqlHelper;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class providing test data generation methods */
public final class TestDataHelper {

  private static final Logger logger = LoggerFactory.getLogger(TestBase.class);

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
    logger.info(
        format("Generated %s new Users:\n%s", sqlQueries.size(), Joiner.on("\n").join(sqlQueries)));
  }
}
