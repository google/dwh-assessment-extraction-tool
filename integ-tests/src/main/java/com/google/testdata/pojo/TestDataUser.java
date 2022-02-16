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
package com.google.testdata.pojo;

import static com.google.testdata.TestDataHelper.getRandomUsername;
import static java.util.UUID.randomUUID;

/** Generates and stores user data for performance tests. */
public final class TestDataUser {

  private final String username;
  private final String password;

  public TestDataUser() {
    this.username = getRandomUsername();
    this.password = randomUUID().toString();
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}
