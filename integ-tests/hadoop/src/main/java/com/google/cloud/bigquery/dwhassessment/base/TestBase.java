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
package com.google.cloud.bigquery.dwhassessment.base;

import com.google.cloud.bigquery.dwhassessment.proto.HiveSchema.Schema;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class with general values for all Junit test suites */
public abstract class TestBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestBase.class);

  /**
   * @param hiveSet db schemas extracted from hive
   * @param dumperSet db schemas extracted from CW dumper file
   */
  public void assertSetsAreEqual(ImmutableSet<Schema> hiveSet, ImmutableSet<Schema> dumperSet) {

    SetView<Schema> missingSchemas = Sets.difference(hiveSet, dumperSet);
    SetView<Schema> additionalSchemas = Sets.difference(dumperSet, hiveSet);

    if (missingSchemas.size() == 0 && additionalSchemas.size() == 0) {
      LOGGER.info("DB view and CW dumper file are equal");
    } else if (missingSchemas.size() != 0 && additionalSchemas.size() != 0) {
      Assert.fail(
          String.format(
              "DB view and Avro file have mutually exclusive row(s)%n"
                  + "DB view has %d different schemas: %n%s"
                  + "Dumper file has %d different schemas: %n%s",
              additionalSchemas.size(),
              getJsonStringFromProtoSet(additionalSchemas),
              missingSchemas.size(),
              getJsonStringFromProtoSet(missingSchemas)));
    } else if (missingSchemas.size() != 0) {
      Assert.fail(
          String.format(
              "DB view has %d extra schemas:%n%s",
              missingSchemas.size(), getJsonStringFromProtoSet(missingSchemas)));
    } else if (additionalSchemas.size() != 0) {
      Assert.fail(
          String.format(
              "CW dumper file has %d extra row(s):%n%s",
              additionalSchemas.size(), getJsonStringFromProtoSet(additionalSchemas)));
    }
  }

  private String getJsonStringFromProtoSet(SetView<Schema> schemas) {
    StringBuilder jsonString = new StringBuilder();
    for (Schema schema : schemas) {
      try {
        jsonString.append(JsonFormat.printer().print(schema)).append(System.lineSeparator());
      } catch (InvalidProtocolBufferException e) {
        LOGGER.error(String.format("Cannot transform protobuf:%n%s", schema.toString()), e);
      }
    }
    return jsonString.toString();
  }
}
