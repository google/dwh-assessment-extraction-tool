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

import static com.google.base.TestBase.TRAILING_SPACES_REGEX;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

/** A helper class for reading and extracting data from Avro files. */
public final class AvroUtil {

  private AvroUtil() {}

  /**
   * @param path Path to the folder with Extraction tool output files.
   * @return The avro schema object.
   */
  public static DataFileReader<GenericRecord> extractAvroDataFromFile(String path)
      throws IOException {
    return new DataFileReader<>(new File(path), new GenericDatumReader<>());
  }

  /**
   * @param record GenericRecord containing value.
   * @param columnName Database column name.
   * @return String or an empty string if null.
   */
  public static String getStringNotNull(GenericRecord record, String columnName) {
    return record.get(columnName) == null
        ? ""
        : TRAILING_SPACES_REGEX.matcher(record.get(columnName).toString()).replaceFirst("");
  }

  /**
   * @param record GenericRecord containing value.
   * @param columnName Database column name.
   * @return int or 0 if null.
   */
  public static int getIntNotNull(GenericRecord record, String columnName) {
    return record.get(columnName) == null ? 0 : parseInt(record.get(columnName).toString());
  }

  /** @return long or 0L if null. */
  public static long getLongNotNull(GenericRecord record, String columnName) {
    return record.get(columnName) == null ? 0L : parseLong(record.get(columnName).toString());
  }

  /**
   * @param record GenericRecord containing value.
   * @param columnName Database column name.
   * @return byte[] or empty byte[] if null.
   */
  public static byte[] getBytesNotNull(GenericRecord record, String columnName) {
    return record.get(columnName) == null
        ? new byte[0]
        : ((ByteBuffer) record.get(columnName)).array();
  }

  /** @return double or 0.0 if null. */
  public static double getDoubleNotNull(GenericRecord record, String columnName) {
    return record.get(columnName) == null ? 0.0 : parseDouble(record.get(columnName).toString());
  }
}
