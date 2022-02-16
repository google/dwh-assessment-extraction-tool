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
import static java.lang.System.lineSeparator;

import com.google.common.base.Joiner;
import com.google.common.collect.LinkedHashMultiset;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class with general values for all Junit test suites */
public abstract class TestBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestBase.class);

  /**
   * @param dbList List of extracted from DB items
   * @param avroList List of uploaded from Avro items
   * @param sqlPath Path to an SQL request that queries from DB
   * @param avroFilePath Path to exported Avro file
   */
  public static void assertListsEqual(
      final LinkedHashMultiset dbList,
      final LinkedHashMultiset avroList,
      String sqlPath,
      String avroFilePath) {
    String dbListOutput = lineSeparator() + Joiner.on("").join(dbList);
    String avroListOutput = lineSeparator() + Joiner.on("").join(avroList);

    if (dbList.isEmpty() && avroList.isEmpty()) {
      LOGGER.info("DB view and Avro file are equal");
    } else if (!dbList.isEmpty() && !avroList.isEmpty()) {
      Assert.fail(
          format(
              "DB view and Avro file have mutually exclusive row(s)%n"
                  + "DB view '%s' has %d different row(s): %s%n"
                  + "Avro file %s has %d different row(s): %s",
              sqlPath, dbList.size(), dbListOutput, avroFilePath, avroList.size(), avroListOutput));
    } else if (!dbList.isEmpty()) {
      Assert.fail(
          format("DB view '%s' has %d extra row(s):%n%s", sqlPath, dbList.size(), dbListOutput));
    } else if (!avroList.isEmpty()) {
      Assert.fail(
          format(
              "Avro file %s has %d extra row(s):%n%s",
              avroFilePath, avroList.size(), avroListOutput));
    }
  }
}
