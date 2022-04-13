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
package com.google.cloud.bigquery.dwhassessment.pojo;

import static java.lang.System.nanoTime;

import java.util.Random;

/** Generates test data to be populated in db. */
public class TestData {

  private final int id;
  private final String name;
  private final String description;
  private final String state;

  public TestData() {
    Random rnd = new Random();
    this.id = rnd.nextInt(Integer.MAX_VALUE);
    this.name = "name_" + nanoTime();
    this.description = "desc_" + nanoTime();
    this.state = "STATE_" + nanoTime();
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getState() {
    return state;
  }
}
