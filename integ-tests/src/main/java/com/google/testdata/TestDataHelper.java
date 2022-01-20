package com.google.testdata;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.lang.System.nanoTime;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.sql.SqlHelper;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class providing test data generation methods */
public final class TestDataHelper {

  private static final Logger logger = LoggerFactory.getLogger(TestDataHelper.class);

  private TestDataHelper() {}

  /**
   * @param connection DB connection parameter
   * @param userCount Repetition count
   * @throws SQLException
   */
  public static void generateUsers(Connection connection, int userCount) throws SQLException {
    final String sqlDataPath = "src/main/java/com/google/sql/data/users_data.sql";
    final String password = UUID.randomUUID().toString();

    ImmutableList<String> sqlQueries =
        Stream.generate(memoize(() -> SqlHelper.getSql(sqlDataPath)))
            .limit(userCount)
            .map(sqlQuery -> format(sqlQuery, nanoTime(), password))
            .collect(toImmutableList());

    SqlHelper.executeQueries(connection, sqlQueries);

    logger.info(
        format(
            "Generated %s new Users:\n%s",
            sqlQueries.size(), Joiner.on("\n").join(sqlQueries).replaceAll(password, "####")));
  }
}
