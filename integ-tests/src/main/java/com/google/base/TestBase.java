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
package com.google.base;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static java.lang.String.format;
import static java.lang.System.getenv;

/** Base class with general values for all Junit test suites */
public abstract class TestBase {

  public static final String DB_NAME = getenv("TD_DB");
  public static final String USERNAME_DB = getenv("TD_USR");
  public static final String PASSWORD_DB = getenv("TD_PSW");
  public static final String URL_DB = format("jdbc:teradata://%s/DBS_PORT=1025", getenv("TD_HOST"));
  public static final String ET_OUTPUT_PATH = getenv("EXPORT_PATH");

  /**
   * @param dateString Date passed as String.
   * @return Date as a timestamp as Long.
   */
  public static long getTimestampByDate(String dateString) {
    if (dateString.equals("")) {
      return 0;
    } else {
      try {
        SimpleDateFormat datetimeFormatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date date = datetimeFormatter1.parse(dateString);
        return date.getTime();
      } catch (ParseException exception) {
        throw new IllegalStateException(
            "Date format doesn't correspond to 'yyyy-MM-dd HH:mm:ss.SSS'", exception);
      }
    }
  }
}
