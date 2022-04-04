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

/** Stores constants common among all tests. */
public final class Constants {

  public static final String HIVE_HOST = getenv("HIVE_HOST");
  public static final String DB_NAME = "default";
  public static final String USERNAME_DB = getenv("HIVE_USR");;
  public static final String PASSWORD_DB = getenv("HIVE_PSW");
  public static final String URL_DB = format("jdbc:hive2://%s:10000/%s", HIVE_HOST, DB_NAME);

  public static final String SQL_TEST_DATA_BASE_PATH = "src/main/java/com/google/sql/";

  private Constants() {}
}
