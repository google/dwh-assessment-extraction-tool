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
package com.google.cloud.bigquery.dwhassessment.extractiontool.dbscripts;

import static com.google.common.base.Charsets.UTF_8;

import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class InternalScriptLoader implements ScriptLoader {

  private static final Set<String> scriptNames =
      ImmutableSet.of(
          "all_ri_children",
          "all_ri_parents",
          "columns",
          "diskspace",
          "functioninfo",
          "indices",
          "partitioning_constraints",
          "query_references",
          "querylogs",
          "roles",
          "stats",
          "tableinfo",
          "tablesize",
          "temp_tables",
          "users");

  @Override
  public ImmutableMap<String, Supplier<String>> loadScripts() {
    return scriptNames.stream()
        .collect(
            ImmutableMap.toImmutableMap(Functions.identity(), key -> scriptLoader(key + ".sql")));
  }

  @Override
  public ImmutableMap<String, ImmutableList<String>> getSortingColumnsMap() {
    return new ImmutableMap.Builder<String, ImmutableList<String>>()
        .put("querylogs.sql", ImmutableList.of("\"{{baseDatabase}}\".\"QryLogV\".\"StartTime\""))
        .build();
  }

  private Supplier<String> scriptLoader(String name) {
    URL scriptUrl = ScriptLoader.class.getResource(name);
    Preconditions.checkArgument(scriptUrl != null, "Resource '%s' does not exist.", name);
    return () -> {
      try {
        return CharStreams.toString(new InputStreamReader(scriptUrl.openStream(), UTF_8));
      } catch (IOException e) {
        throw new IllegalStateException(String.format("Error reading script '%s'.", name), e);
      }
    };
  }
}
