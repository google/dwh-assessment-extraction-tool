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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

/** Interface to manage data entity, e.g. AVRO files. */
public interface DataEntityManager extends Closeable {

  /**
   * Get the output stream for an entity.
   *
   * @param name The name of the output entity.
   */
  OutputStream getEntityOutputStream(String name) throws IOException;

  /**
   * Indicate whether the data entity allows resumable processing.
   *
   * @return true if some progress can be retained after the writing is interrupted.
   */
  boolean isResumable();

  /**
   * Get the absolute path of a file given its name.
   *
   * @param name The name of the file.
   */
  Path getAbsolutePath(String name);
}
