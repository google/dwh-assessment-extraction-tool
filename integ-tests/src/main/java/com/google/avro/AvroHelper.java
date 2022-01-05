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
package com.google.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;

public final class AvroHelper {

  /** A helper class for reading and extracting data from Avro files. */
  private AvroHelper() {}

  /**
   * @param path Path to the folder with Extraction tool output files.
   * @return The avro schema object.
   * @throws IOException
   */
  public static DataFileReader<GenericRecord> extractAvroDataFromFile(String path)
      throws IOException {
    File fileAvro = new File(path);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    return new DataFileReader<>(fileAvro, datumReader);
  }

  /**
   * @param record GenericRecord containing value.
   * @param columnName Database column name.
   * @return String or an empty string.
   */
  public static String getStringNotNull(GenericRecord record, String columnName) {
    if (record.get(columnName) == null) {
      return "";
    } else {
      return record.get(columnName).toString();
    }
  }

  public static Long getLongNotNull(GenericRecord record, String columnName) {
    if (record.get(columnName) == null) {
      return 0L;
    } else {
      return Long.parseLong(record.get(columnName).toString());
    }
  }
}
