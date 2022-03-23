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
package com.google.testdata;

import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.UUID.randomUUID;

/** Common methods for Test Data Helpers. */
public final class TestDataHelper {

  private TestDataHelper() {}

  /** @return Random name for creating a DB */
  public static String getRandomDbName() {
    return format("test_db_%s", nanoTime());
  }

  /** @return Random name for creating a Table */
  public static String getRandomTableName() {
    return format("test_table_%s", nanoTime());
  }

  /** @return Random name for creating a User */
  public static String getRandomUsername() {
    return format("test_user_%s", nanoTime());
  }

  /** @return Random name for creating a Function */
  public static String getRandomFunctionName() {
    return format("test_func_%s", nanoTime());
  }

  /** @return Random name for creating a Role */
  public static String getRandomRolename() {
    return format("test_role_%s", nanoTime());
  }
}
