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

package com.google.cloud.bigquery.dwhassessment.extractiontool.dumper;

import java.io.IOException;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;


/**
 * Implementation of a DataEntityManger for unit test purposes.
 */
public class DataEntityManagerTesting implements DataEntityManager {

  private final ByteArrayOutputStream outputStream;

  /**
   * Constructs a new DataEntityManagerTesting.
   */
  public DataEntityManagerTesting(ByteArrayOutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public OutputStream getEntityOutputStream(String name) {
    return this.outputStream;
  }

  @Override
  public boolean isResumable() {
    return false;
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
  }

}
