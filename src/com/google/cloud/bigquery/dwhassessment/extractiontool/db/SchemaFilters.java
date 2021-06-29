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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import java.util.Map;

/** Utility methods to deal with schema filters. */
public final class SchemaFilters {

  private static final String KEY_DB = "db";
  private static final String KEY_TABLE = "table";
  private static final ImmutableSet<String> VALID_KEYS = ImmutableSet.of(KEY_DB, KEY_TABLE);

  private SchemaFilters() {}

  /** Parses a string representation of a schema filter. */
  public static SchemaFilter parse(String input) {
    Map<String, String> filterMap =
        "".equals(input)
            ? ImmutableMap.of()
            : Splitter.onPattern(",\\s*").withKeyValueSeparator(':').split(input);
    Sets.SetView<String> unknownKeys = Sets.difference(filterMap.keySet(), VALID_KEYS);
    Preconditions.checkState(
        unknownKeys.isEmpty(),
        "Got filter with invalid key(s): %s.",
        Joiner.on(", ").join(unknownKeys));

    SchemaFilter.Builder filterBuilder = SchemaFilter.builder();
    if (filterMap.containsKey(KEY_DB)) {
      filterBuilder.setDatabaseName(compilePattern(KEY_DB, filterMap.get(KEY_DB)));
    }
    if (filterMap.containsKey(KEY_TABLE)) {
      filterBuilder.setTableName(compilePattern(KEY_TABLE, filterMap.get(KEY_TABLE)));
    }
    return filterBuilder.build();
  }

  private static Pattern compilePattern(String key, String pattern) {
    try {
      return Pattern.compile(pattern);
    } catch (PatternSyntaxException e) {
      throw new IllegalStateException(
          String.format("The pattern '%s' for filter key '%s' is invalid.", pattern, key), e);
    }
  }
}
