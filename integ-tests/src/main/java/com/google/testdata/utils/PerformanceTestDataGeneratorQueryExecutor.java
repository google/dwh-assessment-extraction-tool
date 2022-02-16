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
package com.google.testdata.utils;

import static com.google.sql.SqlHelper.connectAndExecuteQueryAsUser;
import static java.lang.String.format;
import static java.lang.System.lineSeparator;

import com.google.common.base.Joiner;
import com.google.testdata.pojo.TestDataUser;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.RecursiveAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes queries for each user in separate thread. */
public class PerformanceTestDataGeneratorQueryExecutor extends RecursiveAction {

  private final HashMap<TestDataUser, List<String>> performancePayload;
  private final int startIndex;
  private final int lastIndex;
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PerformanceTestDataGeneratorQueryExecutor.class);

  public PerformanceTestDataGeneratorQueryExecutor(
      HashMap<TestDataUser, List<String>> performancePayload, int startIndex, int lastIndex) {
    this.performancePayload = performancePayload;
    this.startIndex = startIndex;
    this.lastIndex = lastIndex;
  }

  private void executeQueries(TestDataUser userData, List<String> queries) {
    queries.forEach(
        e -> {
          try {
            connectAndExecuteQueryAsUser(userData.getUsername(), userData.getPassword(), e);
          } catch (SQLException ex) {
            LOGGER.error(
                format("Error on executing query for user %s%n%s", userData.getUsername(), ex));
          }
        });
    LOGGER.info(
        format(
            "Queries executed by user %s%n%s",
            userData.getUsername(), Joiner.on(lineSeparator()).join(queries)));
  }

  @Override
  protected void compute() {
    int length = lastIndex - startIndex;
    int THRESHOLD = 1;
    if (length <= THRESHOLD) {
      TestDataUser userData = (TestDataUser) performancePayload.keySet().toArray()[startIndex];
      executeQueries(userData, performancePayload.get(userData));
      return;
    }
    PerformanceTestDataGeneratorQueryExecutor firstTaskBreakdown =
        new PerformanceTestDataGeneratorQueryExecutor(
            performancePayload, startIndex, (startIndex + lastIndex) / 2);
    firstTaskBreakdown.fork();
    PerformanceTestDataGeneratorQueryExecutor secondTaskBreakdown =
        new PerformanceTestDataGeneratorQueryExecutor(
            performancePayload, (startIndex + lastIndex) / 2, lastIndex);
    secondTaskBreakdown.compute();
    firstTaskBreakdown.join();
  }
}
