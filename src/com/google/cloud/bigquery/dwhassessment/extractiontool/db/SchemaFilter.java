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
package com.google.cloud.bigquery.dwhassessment.extractiontool.db;

import com.google.auto.value.AutoValue;
import com.google.re2j.Pattern;
import java.util.Optional;

/**
 * A filter for DB schemas, i.e. it defines attributes a schema needs to fulfill to match the
 * filter.
 */
@AutoValue
public abstract class SchemaFilter {

  public abstract Optional<Pattern> databaseName();

  public abstract Optional<Pattern> tableName();

  public static Builder builder() {
    return new AutoValue_SchemaFilter.Builder();
  }

  /** Builder for the SchemaFilter. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setDatabaseName(Pattern databaseName);

    public abstract Builder setTableName(Pattern tableName);

    public abstract SchemaFilter build();
  }
}
