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

import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.HandlebarsException;
import com.github.jknack.handlebars.Helper;
import com.github.jknack.handlebars.Template;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SqlScriptVariables.QueryLogsVariables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqlTemplateRendererImpl implements SqlTemplateRenderer {

  private final Handlebars handlebars = new Handlebars();
  private final SqlScriptVariables.Builder sqlScriptVariablesBuilder;

  public SqlTemplateRendererImpl(SqlScriptVariables.Builder sqlScriptVariablesBuilder) {
    this.sqlScriptVariablesBuilder = sqlScriptVariablesBuilder;
    handlebars.registerHelper(
        "whereClauseForQuerylogs",
        (Helper<QueryLogsVariables>)
            (queryLogsVariables, options) -> {
              List<String> rangeClauses = new ArrayList<>();
              queryLogsVariables
                  .timeRange()
                  .ifPresent(
                      timeRange -> {
                        rangeClauses.add(
                            String.format(
                                "\"QLV\".\"StartTime\" BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'",
                                timeRange.getStartTimestamp(), timeRange.getEndTimestamp()));
                      });
              if (!queryLogsVariables.users().isEmpty()) {
                rangeClauses.add(
                    String.format(
                        "\"QLV\".\"UserName\" IN %s",
                        queryLogsVariables.users().stream()
                            .collect(Collectors.joining("','", "('", "')"))));
              }
              if (!rangeClauses.isEmpty()) {
                return String.format("\nWHERE\n%s", String.join("\nAND\n", rangeClauses));
              }
              return "";
            });
  }

  @Override
  public String renderTemplate(String name, String sql) {
    Template template;
    try {
      template = handlebars.compileInline(sql);
    } catch (HandlebarsException | IOException e) {
      throw new IllegalStateException(String.format("Failed to compile SQL template '%s'.", name));
    }
    try {
      return template.apply(sqlScriptVariablesBuilder.build());
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Failed to apply SQL template '%s'.", name));
    }
  }

  @Override
  public SqlScriptVariables.Builder getSqlScriptVariablesBuilder() {
    return sqlScriptVariablesBuilder;
  }
}
