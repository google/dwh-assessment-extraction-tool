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
package com.google.cloud.bigquery.dwhassessment.dumper;

import com.google.cloud.bigquery.dwhassessment.base.Constants;
import com.google.common.collect.ImmutableSet;
import com.google.cloud.bigquery.dwhassessment.proto.HiveSchema.Schema;
import com.google.protobuf.util.JsonFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A helper class for reading and extracting data from dumper files. */
public class DumperUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(DumperUtils.class);

  private DumperUtils() {}

  /**
   * @return list of pojo database schemas
   */
  public static ImmutableSet<Schema> readSchemas() throws IOException {
    ZipFile zipFile = new ZipFile(Constants.EXPORT_PATH);
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    ImmutableSet.Builder<Schema> schemasBuilder = new ImmutableSet.Builder<>();
    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      if (entry.getName().equals(Constants.SCHEMA_FILE)) {
        BufferedReader br =
            new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry)));
        String line;
        while ((line = br.readLine()) != null) {
          Schema.Builder schemaBuilder = Schema.newBuilder();
          JsonFormat.parser().ignoringUnknownFields().merge(line, schemaBuilder);
          Schema schema = schemaBuilder.build();
          schemasBuilder.add(schema);
        }
        break;
      }
    }
    return schemasBuilder.build();
  }
}
