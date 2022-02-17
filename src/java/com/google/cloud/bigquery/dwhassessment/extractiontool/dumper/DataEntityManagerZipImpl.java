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
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/** Implementation of DataEntityManager that stores data entity files in a zip archive. */
public class DataEntityManagerZipImpl implements DataEntityManager {

  private final ZipOutputStream zipOutputStream;
  private final AtomicBoolean isEntityOutputStreamOpen;

  /**
   * Constructs a new DataEntityManagerArchiveImpl.
   *
   * @param outputStream outputStream where the zip archive will be written. This will not be
   *     closed.
   */
  public DataEntityManagerZipImpl(OutputStream outputStream) {
    this(new ZipOutputStream(outputStream));
  }

  public DataEntityManagerZipImpl(ZipOutputStream zipOutputStream) {
    this.zipOutputStream = zipOutputStream;
    isEntityOutputStreamOpen = new AtomicBoolean(false);
  }

  @Override
  public OutputStream getEntityOutputStream(String name) {
    if (!isEntityOutputStreamOpen.compareAndSet(false, true)) {
      throw new IllegalStateException(
          "Previous EntityOutputStream is not closed. "
              + "Only one opened EntityOutputStream can exist concurrently.");
    }
    try {
      return new ZipEntryOutputStream(name);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public boolean isResumable() {
    return false;
  }

  @Override
  public Path getAbsolutePath(String name) {
    return null;
  }

  @Override
  public void close() throws IOException {
    zipOutputStream.close();
  }

  /** OutputStream that writes a single file to the parent ZipOutputStream. */
  private class ZipEntryOutputStream extends OutputStream {

    public ZipEntryOutputStream(String name) throws IOException {
      ZipEntry zipEntry = new ZipEntry(name);
      zipOutputStream.putNextEntry(zipEntry);
    }

    @Override
    public void write(int b) throws IOException {
      zipOutputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      zipOutputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      zipOutputStream.write(b, off, len);
    }

    @Override
    public void close() {
      isEntityOutputStreamOpen.set(false);
    }
  }
}
