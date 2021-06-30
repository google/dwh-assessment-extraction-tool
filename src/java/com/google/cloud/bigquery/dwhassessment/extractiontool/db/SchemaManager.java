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
import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManager;
import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.util.List;

/** Interface to manage database schemas. */
public interface SchemaManager {

  /** Identifier for a schema. */
  @AutoValue
  abstract class SchemaKey {
    public static SchemaKey create(String databaseName, String tableName) {
      return new AutoValue_SchemaManager_SchemaKey(databaseName, tableName);
    }

    public abstract String databaseName();

    public abstract String tableName();
  }

  /** Retrieves the given schema and writes it via the data entity manager. */
  void retrieveSchema(
      Connection connection, SchemaKey schemaKey, DataEntityManager dataEntityManager);

  /** Gets a list of names of matching */
  ImmutableSet<SchemaKey> getSchemaKeys(Connection connection, List<SchemaFilter> filters);
}
