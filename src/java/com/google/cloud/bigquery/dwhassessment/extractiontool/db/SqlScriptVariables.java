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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.List;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;

@AutoValue
public abstract class SqlScriptVariables {

  public static Builder builder() {
    return new AutoValue_SqlScriptVariables.Builder()
        .setBaseDatabase("DBC")
        .setVars(ImmutableMap.of())
        .setSortingColumns(ImmutableList.of());
  }

  public abstract String getBaseDatabase();

  public abstract List<String> getSortingColumns();

  public abstract QueryLogsVariables getQueryLogsVariables();

  public abstract Map<String, String> getVars();

  @AutoValue
  public abstract static class QueryLogsVariables {

    public static Builder builder() {
      return new AutoValue_SqlScriptVariables_QueryLogsVariables.Builder();
    }

    public abstract Optional<TimeRange> timeRange();

    // Value accessor for handlebars.
    public TimeRange getTimeRange() {
      if (timeRange().isPresent()) {
        return timeRange().get();
      }
      return null;
    }

    @AutoValue
    public abstract static class TimeRange {
      private static final String minTime = "0001-01-01 00:00:00";
      private static final String maxTime = "9999-12-31 23:59:59.99";

      public static Builder builder() {
        return new AutoValue_SqlScriptVariables_QueryLogsVariables_TimeRange.Builder()
            .setStartTimestamp(minTime)
            .setEndTimestamp(maxTime);
      }

      public abstract String getStartTimestamp();

      public abstract String getEndTimestamp();

      @AutoValue.Builder
      public abstract static class Builder {
        public abstract Builder setStartTimestamp(String timestamp);

        public abstract Builder setEndTimestamp(String timestamp);

        public abstract TimeRange build();
      }
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTimeRange(TimeRange value);

      public abstract QueryLogsVariables build();
    }
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setBaseDatabase(String value);

    public abstract Builder setSortingColumns(List<String> value);

    public abstract Builder setQueryLogsVariables(QueryLogsVariables value);

    public abstract Builder setVars(Map<String, String> variables);

    public abstract SqlScriptVariables build();
  }
}
