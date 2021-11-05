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
package com.google.cloud.bigquery.dwhassessment.extractiontool.executor;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilter;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/** Executor for the extract action. */
public interface ExtractExecutor {

  /** Arguments for the extract action. */
  @AutoValue
  abstract class Arguments {
    /** The JDBC address of the database to which to connect. */
    public abstract String dbConnectionAddress();

    /** The JDBC connection properties. */
    public abstract Properties dbConnectionProperties();

    /** The path to which to write the output. Must be a directory. */
    public abstract Path outputPath();

    /** SQL scripts to run. */
    public abstract ImmutableList<String> sqlScripts();

    /** SQL scripts to exclude (i.e., run the full catalog except for these scripts). */
    public abstract ImmutableList<String> skipSqlScripts();

    /** Filter to apply on schemas to extract */
    public abstract ImmutableList<SchemaFilter> schemaFilters();

    /** The base database from which to extract the metadata. */
    public abstract String baseDatabase();

    /** Whether to do a dry run. */
    public abstract boolean dryRun();

    public abstract Optional<Instant> qryLogStartTime();

    public abstract Optional<Instant> qryLogEndTime();

    public static Builder builder() {
      return new AutoValue_ExtractExecutor_Arguments.Builder()
          .setDryRun(false)
          .setBaseDatabase("DBC")
          .setSchemaFilters(ImmutableList.of())
          .setSqlScripts(ImmutableList.of())
          .setSkipSqlScripts(ImmutableList.of());
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDbConnectionProperties(Properties properties);

      public abstract Builder setDbConnectionAddress(String dbAddress);

      public abstract Builder setOutputPath(Path path);

      public abstract Builder setSqlScripts(List<String> scripts);

      public abstract Builder setSkipSqlScripts(List<String> scripts);

      public abstract Builder setSchemaFilters(List<SchemaFilter> schemaFilters);

      public abstract Builder setBaseDatabase(String baseDatabase);

      public abstract Builder setDryRun(boolean dryRun);

      public abstract Builder setQryLogStartTime(Instant timestampInUtc);

      public abstract Builder setQryLogEndTime(Instant timestampInUtc);

      public abstract Arguments build();
    }
  }

  /** Run the extract executor. */
  int run(Arguments arguments) throws SQLException, IOException;
}
