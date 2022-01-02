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

import static com.google.common.truth.Truth.assertThat;

import java.io.ByteArrayOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.junit.Test;

public class AvroResultSetRecorderTest {

  @Test
  public void add_successful() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("Record").fields();
    fields = fields.name("field_one").type().intType().noDefault();
    fields = fields.name("field_two").type().stringType().noDefault();
    Schema schema = fields.endRecord();

    try (ResultSetRecorder<GenericRecord> recorder =
        AvroResultSetRecorder.create(schema, outputStream)) {
      recorder.add(
          new GenericRecordBuilder(schema).set("field_one", 23).set("field_two", "abc").build());
      recorder.add(
          new GenericRecordBuilder(schema).set("field_one", 42).set("field_two", "def").build());
    }

    DatumReader<Record> datumReader = new GenericDatumReader<>();
    DataFileReader<Record> dataFileReader =
        new DataFileReader<>(new SeekableByteArrayInput(outputStream.toByteArray()), datumReader);

    assertThat(dataFileReader.next())
        .isEqualTo(
            new GenericRecordBuilder(schema).set("field_one", 23).set("field_two", "abc").build());
    assertThat(dataFileReader.next())
        .isEqualTo(
            new GenericRecordBuilder(schema).set("field_one", 42).set("field_two", "def").build());
    assertThat(dataFileReader.hasNext()).isFalse();
  }
}
