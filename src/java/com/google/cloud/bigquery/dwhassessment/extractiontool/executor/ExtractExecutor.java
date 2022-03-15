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
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/** Executor for the extract action. */
public interface ExtractExecutor {

  enum RunMode {
    NORMAL,
    INCREMENTAL
  }

  /** Arguments for the extract action. */
  @AutoValue
  abstract class Arguments {
    /** The JDBC address of the database to which to connect. */
    public abstract String dbConnectionAddress();

    /** The JDBC connection properties. */
    public abstract Properties dbConnectionProperties();

    /** The path to which to write the output. Must be a directory. */
    public abstract Path outputPath();

    /** The path containing records from previous run(s). Must be a directory. */
    public abstract Optional<Path> prevRunPath();

    /** Supported modes: NORMAL, INCREMENTAL. */
    public abstract RunMode mode();

    /** Whether to extract query texts. */
    public abstract boolean needQueryText();

    /** SQL scripts to run. */
    public abstract ImmutableList<String> sqlScripts();

    /** Optional variables for the scripts. */
    public abstract ImmutableMap<String, Map<String, String>> scriptVariables();

    /** SQL scripts to exclude (i.e., run the full catalog except for these scripts). */
    public abstract ImmutableList<String> skipSqlScripts();

    /** Extract schemas via JDBC. */
    public abstract boolean needJdbcSchemas();

    /** Filter to apply on schemas to extract */
    public abstract ImmutableList<SchemaFilter> schemaFilters();

    /** The base database from which to extract the metadata. */
    public abstract String baseDatabase();

    /** Base DB overwrites per script. */
    public abstract ImmutableMap<String, String> scriptBaseDatabase();

    /** Whether to do a dry run. */
    public abstract boolean dryRun();

    /** Number of records per chunk file (if chunk mode is available). */
    public abstract Integer chunkRows();

    public abstract Optional<Instant> qryLogStartTime();

    public abstract Optional<Instant> qryLogEndTime();

    public static Builder builder() {
      return new AutoValue_ExtractExecutor_Arguments.Builder()
          .setDryRun(false)
          .setBaseDatabase("DBC")
          .setChunkRows(0)
          .setMode(RunMode.NORMAL)
          .setNeedQueryText(true)
          .setScriptVariables(ImmutableMap.of())
          .setScriptBaseDatabase(ImmutableMap.of())
          .setNeedJdbcSchemas(true)
          .setSchemaFilters(ImmutableList.of())
          .setSqlScripts(ImmutableList.of())
          .setSkipSqlScripts(ImmutableList.of());
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDbConnectionProperties(Properties properties);

      public abstract Builder setDbConnectionAddress(String dbAddress);

      public abstract Builder setOutputPath(Path path);

      public abstract Builder setPrevRunPath(Path path);

      public abstract Builder setMode(RunMode mode);

      public abstract Builder setNeedQueryText(boolean value);

      public abstract Builder setSqlScripts(List<String> scripts);

      public abstract Builder setSkipSqlScripts(List<String> scripts);

      public abstract Builder setScriptVariables(
          ImmutableMap<String, Map<String, String>> variables);

      public abstract Builder setNeedJdbcSchemas(boolean needJdbcSchemas);

      public abstract Builder setSchemaFilters(List<SchemaFilter> schemaFilters);

      public abstract Builder setBaseDatabase(String baseDatabase);

      public abstract Builder setScriptBaseDatabase(
          ImmutableMap<String, String> scriptBaseDatabase);

      public abstract Builder setDryRun(boolean dryRun);

      public abstract Builder setChunkRows(Integer chunkRows);

      public abstract Builder setQryLogStartTime(Instant timestampInUtc);

      public abstract Builder setQryLogEndTime(Instant timestampInUtc);

      public abstract Arguments build();
    }
  }

  /** Run the extract executor. */
  int run(Arguments arguments) throws SQLException, IOException;
}
