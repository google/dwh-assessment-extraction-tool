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
package com.google.tdjdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * A helper class for checking Null values returned by executing SELECT request against a database.
 */
public final class JdbcHelper {

  private static final Pattern TRAILING_SPACES_REGEX = Pattern.compile("\\s++$");

  /**
   * @param rs A row with SELECT results.
   * @param column Database column name.
   * @return String or an empty string if null.
   * @throws SQLException
   */
  public static String getStringNotNull(ResultSet rs, String column) throws SQLException {
    String string = rs.getString(column);
    if (rs.wasNull()) {
      return "";
    } else {
      return TRAILING_SPACES_REGEX.matcher(string).replaceFirst("");
    }
  }

  /**
   * @param rs A row with SELECT results.
   * @param column Database column name.
   * @return int or 0 if null.
   * @throws SQLException
   */
  public static int getIntNotNull(ResultSet rs, String column) throws SQLException {
    int integer = rs.getInt(column);
    if (rs.wasNull()) {
      return 0;
    } else {
      return integer;
    }
  }

  /**
   * @param rs A row with SELECT results.
   * @param column Database column name.
   * @return long or 0L if null.
   * @throws SQLException
   */
  public static long getLongNotNull(ResultSet rs, String column) throws SQLException {
    long longValue = rs.getLong(column);
    if (rs.wasNull()) {
      return 0L;
    } else {
      return longValue;
    }
  }
}
