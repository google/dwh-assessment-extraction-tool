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
 * package com.google.cloud.bigquery.dwhassessment.extractiontool.config;
 */
package com.google.cloud.bigquery.dwhassessment.extractiontool.executor;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilter;
import com.google.common.collect.ImmutableList;
import java.nio.file.Path;
import java.util.List;

/** Executor for the extract action. */
public interface ExtractExecutor {

  /** Arguments for the extract action. */
  @AutoValue
  abstract class Arguments {
    /** The JDBC address of the database to which to connect. */
    public abstract String dbAddress();

    /** The path to which to write the output. Must be a directory. */
    public abstract Path outputPath();

    /** SQL scripts to run. */
    public abstract ImmutableList<String> sqlScripts();

    /** SQL scripts to exclude (i.e., run the full catalog except for these scripts). */
    public abstract ImmutableList<String> skipSqlScripts();

    /** Filter to apply on schemas to extract */
    public abstract ImmutableList<SchemaFilter> schemaFilters();

    public static Builder builder() {
      return new AutoValue_ExtractExecutor_Arguments.Builder()
          .setSchemaFilters(ImmutableList.of())
          .setSqlScripts(ImmutableList.of())
          .setSkipSqlScripts(ImmutableList.of());
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDbAddress(String dbAddress);

      public abstract Builder setOutputPath(Path path);

      public abstract Builder setSqlScripts(List<String> scripts);

      public abstract Builder setSkipSqlScripts(List<String> scripts);

      public abstract Builder setSchemaFilters(List<SchemaFilter> schemaFilters);

      public abstract Arguments build();
    }
  }

  /** Run the extract executor. */
  int run(Arguments arguments);
}
