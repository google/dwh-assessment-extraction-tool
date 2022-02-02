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

import static java.lang.String.format;
import static java.lang.System.getenv;

import com.google.common.base.Joiner;
import com.google.common.collect.LinkedHashMultiset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class with general values for all Junit test suites
 */
public abstract class TestBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestBase.class);

  public static final String DB_NAME = getenv("TD_DB");
  public static final String USERNAME_DB = getenv("TD_USR");
  public static final String PASSWORD_DB = getenv("TD_PSW");
  public static final String URL_DB = format("jdbc:teradata://%s/DBS_PORT=1025", getenv("TD_HOST"));
  public static final String ET_OUTPUT_PATH = getenv("EXPORT_PATH");

  public static final String TESTDATA_SQL_PATH = "src/main/java/com/google/sql/testdata/";
  public static final String SQL_PATH = "src/main/java/com/google/sql/";

  public static final Pattern TRAILING_SPACES_REGEX = Pattern.compile("\\s+$");

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

  /**
   * @param dbList List of extracted from DB items
   * @param avroList List of uploaded from Avro items
   * @param sqlPath Path to an SQL request that queries from DB
   * @param avroFilePath Path to exported Avro file
   */
  public static void assertListsEqual(
      final LinkedHashMultiset dbList, final LinkedHashMultiset avroList, String sqlPath,
      String avroFilePath) {
    String dbListOutput = "\n" + Joiner.on("").join(dbList);
    String avroListOutput = "\n" + Joiner.on("").join(avroList);

    if (dbList.isEmpty() && avroList.isEmpty()) {
      LOGGER.info("DB view and Avro file are equal");
    } else if (!dbList.isEmpty() && !avroList.isEmpty()) {
      Assert.fail(
          format(
              "DB view and Avro file have mutually exclusive row(s) \n"
                  + "DB view '%s' has %d different row(s): %s"
                  + "\nAvro file %s has %d different row(s): %s",
              sqlPath, dbList.size(), dbListOutput, avroFilePath, avroList.size(), avroListOutput));
    } else if (!dbList.isEmpty()) {
      Assert.fail(
          format("DB view '%s' has %d extra row(s): \n %s", sqlPath, dbList.size(), dbListOutput));
    } else if (!avroList.isEmpty()) {
      Assert.fail(
          format(
              "Avro file %s has %d extra row(s): \n %s",
              avroFilePath, avroList.size(), avroListOutput));
    }
  }
}
