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

import static com.google.common.base.Preconditions.checkArgument;

import com.github.jknack.handlebars.Options;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SqlScriptVariables.QueryLogsVariables;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SqlScriptVariables.QueryLogsVariables.TimeRange;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class providing helper methods for handlebars templates.
 *
 * <p>All public methods can be used in templates as it is, e.g. {@code {{#whereClauseForQuerylogs
 * queryLogsVariables "QLV"}}{{/whereClauseForQuerylogs}} }
 */
public final class HandlebarsHelpers {
  /** Returns WHERE clause contents specific to querylogs.sql */
  public static CharSequence whereClauseForQuerylogs(
      QueryLogsVariables queryLogsVariables, Options options) {
    String qryLogVAlias = options.param(0);

    checkArgument(!qryLogVAlias.isEmpty(), "qryLogVAlias cannot be empty.");

    List<String> whereClauses = new ArrayList<>();
    queryLogsVariables
        .timeRange()
        .ifPresent(
            timeRange ->
                whereClauses.add(timestampRangeClause(timeRange, qryLogVAlias, "StartTime")));
    if (!queryLogsVariables.users().isEmpty()) {
      whereClauses.add(
          String.format(
              "%s.\"UserName\" IN %s",
              wrapInQuotes(qryLogVAlias),
              queryLogsVariables.users().stream().collect(Collectors.joining("','", "('", "')"))));
    }
    if (!whereClauses.isEmpty()) {
      return String.format("\nWHERE\n%s", String.join("\nAND\n", whereClauses));
    }
    return "";
  }

  /** Returns WHERE clause content for scripts requiring only time range */
  public static CharSequence whereClauseWithTimeRange(
      QueryLogsVariables queryLogsVariables, Options options) {
    String tableAlias = options.param(0);
    String columnAlias = options.param(1);

    checkArgument(!tableAlias.isEmpty(), "tableAlias cannot be empty.");
    checkArgument(!columnAlias.isEmpty(), "columnAlias cannot be empty.");

    return queryLogsVariables
        .timeRange()
        .map(timeRange -> "WHERE " + timestampRangeClause(timeRange, tableAlias, columnAlias))
        .orElse("");
  }

  /**
   * Returns table name with a correct database. If no override for the used script exists,
   * defaultTableName will be used
   */
  public static CharSequence getTableName(String defaultTableName, Options options) {
    SqlScriptVariables model = (SqlScriptVariables) options.context.model();

    return String.format(
        "%s.%s",
        wrapInQuotes(model.getBaseDatabase()),
        wrapInQuotes(model.getVars().getOrDefault("tableName", defaultTableName)));
  }

  private static String timestampRangeClause(
      TimeRange timeRange, String tableAlias, String columnName) {
    return String.format(
        "%s.%s BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'",
        wrapInQuotes(tableAlias),
        wrapInQuotes(columnName),
        timeRange.getStartTimestamp(),
        timeRange.getEndTimestamp());
  }

  private static String wrapInQuotes(String input) {
    return String.format("\"%s\"", input);
  }
}
