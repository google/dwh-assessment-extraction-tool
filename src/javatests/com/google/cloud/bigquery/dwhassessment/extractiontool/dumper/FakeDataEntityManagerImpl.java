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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/** Implementation of a DataEntityManger for unit test purposes. */
public class FakeDataEntityManagerImpl implements DataEntityManager {

  private final Path tmpDir;
  private final ByteArrayOutputStream outputStream;
  private final boolean bareStreamMode;

  /**
   * Constructs a fake DataEntityManager for testing; it supports chunked writing; the output files
   * are saved in a temporary directory.
   */
  public FakeDataEntityManagerImpl(String testDirName) throws IOException {
    this.tmpDir = Files.createTempDirectory(testDirName);
    this.outputStream = null;
    bareStreamMode = false;
  }

  /**
   * Construct a fake DataEntityManager with a fixed reference of ByteArrayOutputStream; it does not
   * support any file operations.
   */
  public FakeDataEntityManagerImpl(ByteArrayOutputStream outputStream) {
    this.outputStream = outputStream;
    this.tmpDir = null;
    bareStreamMode = true;
  }

  @Override
  public OutputStream getEntityOutputStream(String name) throws IOException {
    return bareStreamMode ? outputStream : Files.newOutputStream(tmpDir.resolve(name));
  }

  @Override
  public boolean isResumable() {
    return !bareStreamMode;
  }

  @Override
  public Path getAbsolutePath(String name) {
    return bareStreamMode ? null : tmpDir.resolve(name);
  }

  @Override
  public void close() throws IOException {}
}
