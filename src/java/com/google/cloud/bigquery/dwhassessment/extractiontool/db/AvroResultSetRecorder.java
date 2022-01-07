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
package com.google.cloud.bigquery.dwhassessment.extractiontool.db;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

/** Result set recorder for AVRO output. */
public class AvroResultSetRecorder implements ResultSetRecorder<GenericRecord> {

  private final OutputStream outputStream;
  private final DataFileWriter<GenericRecord> dataFileWriter;

  /**
   * Creates an avro result set recorder.
   *
   * @param schema the schema to be used for the AVRO file.
   * @param outputStream the output stream to which to write.
   * @throws IOException if creating the AVRO file writer failed.
   */
  public static AvroResultSetRecorder create(Schema schema, OutputStream outputStream)
      throws IOException {
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
    dataFileWriter.create(schema, outputStream);
    return new AvroResultSetRecorder(outputStream, dataFileWriter);
  }

  private AvroResultSetRecorder(
      OutputStream outputStream, DataFileWriter<GenericRecord> dataFileWriter) {
    this.outputStream = outputStream;
    this.dataFileWriter = dataFileWriter;
  }

  @Override
  public void add(GenericRecord record) {
    try {
      dataFileWriter.append(record);
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format(
              "Failed to encode query result %s to file with error message: %s",
              record.toString(), e.getMessage()));
    }
  }

  @Override
  public void close() throws IOException {
    dataFileWriter.close();
    outputStream.close();
  }
}
