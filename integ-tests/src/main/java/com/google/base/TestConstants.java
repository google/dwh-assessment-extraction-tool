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

import java.util.regex.Pattern;

/** Stores constants common among all tests. */
public final class TestConstants {

  public static final String DB_NAME = getenv("TD_DB");
  public static final String USERNAME_DB = getenv("TD_USR");
  public static final String PASSWORD_DB = getenv("TD_PSW");
  public static final String URL_DB = format("jdbc:teradata://%s/DBS_PORT=1025", getenv("TD_HOST"));
  public static final String ET_OUTPUT_PATH = getenv("EXPORT_PATH");

  public static final String SQL_TESTDATA_BASE_PATH = "src/main/java/com/google/sql/testdata/";
  public static final String SQL_REQUESTS_BASE_PATH = "src/main/java/com/google/sql/";

  public static final Pattern TRAILING_SPACES_REGEX = Pattern.compile("\\s+$");

  private TestConstants() {}
}
