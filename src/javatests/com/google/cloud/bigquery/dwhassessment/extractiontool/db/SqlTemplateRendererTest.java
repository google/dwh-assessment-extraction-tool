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

import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.SqlScriptVariables.*;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SqlTemplateRendererTest {

  private SqlTemplateRenderer underTest;
  private SqlScriptVariables.Builder baseVariablesBuilder;

  @Before
  public void setUp() {
    baseVariablesBuilder = SqlScriptVariables.builder().setBaseDatabase("test-db");
  }

  @Test
  public void render_base_success() {
    underTest =
        new SqlTemplateRendererImpl(
            baseVariablesBuilder
                .setQueryLogsVariables(QueryLogsVariables.builder().build())
                .build());
    assertThat(
            underTest.renderTemplate(
                /* name= */ "test",
                /* sql= */ "SELECT t.a AS b FROM \"{{baseDatabase}}\".bar AS t"))
        .isEqualTo("SELECT t.a AS b FROM \"test-db\".bar AS t");
  }

  @Test
  public void render_missing_optional_parameter_success() {
    underTest =
        new SqlTemplateRendererImpl(
            baseVariablesBuilder
                .setQueryLogsVariables(QueryLogsVariables.builder().build())
                .build());
    assertThat(
            underTest.renderTemplate(
                /* name= */ "test",
                /* sql= */ "SELECT t.a AS b FROM \"{{baseDatabase}}\".bar"
                    + "{{#if queryLogsVariables.timeRange}}\n"
                    + "WHERE t.timestamp BETWEEN"
                    + " \"{{queryLogsVariables.timeRange.startTimestamp}}\" AND"
                    + " \"{{queryLogsVariables.timeRange.endTimestamp}}\"\n"
                    + "{{/if}}"))
        .isEqualTo("SELECT t.a AS b FROM \"test-db\".bar");
  }

  @Test
  public void render_all_success() {
    underTest =
        new SqlTemplateRendererImpl(
            baseVariablesBuilder
                .setQueryLogsVariables(
                    QueryLogsVariables.builder()
                        .setTimeRange(
                            QueryLogsVariables.TimeRange.builder()
                                .setStartTimestamp("2021-01-01 23:23:46.123456")
                                .setEndTimestamp("2022-01-01 23:23:46")
                                .build())
                        .build())
                .build());
    assertThat(
            underTest.renderTemplate(
                /* name= */ "test",
                /* sql= */ "SELECT t.a AS b FROM \"{{baseDatabase}}\".bar {{#if"
                    + " queryLogsVariables.timeRange}}WHERE t.timestamp BETWEEN TIMESTAMP"
                    + " '{{queryLogsVariables.timeRange.startTimestamp}}' AND TIMESTAMP"
                    + " '{{queryLogsVariables.timeRange.endTimestamp}}'{{/if}}"))
        .isEqualTo(
            "SELECT t.a AS b FROM \"test-db\".bar "
                + "WHERE t.timestamp BETWEEN TIMESTAMP '2021-01-01 23:23:46.123456' "
                + "AND TIMESTAMP '2022-01-01 23:23:46'");
  }

  @Test
  public void render_invalidTemplateThrows() {
    underTest =
        new SqlTemplateRendererImpl(
            baseVariablesBuilder
                .setQueryLogsVariables(QueryLogsVariables.builder().build())
                .build());
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> underTest.renderTemplate(/* name= */ "test", /* sql= */ "{{#foo}}"));
    assertThat(e).hasMessageThat().contains("Failed to compile SQL template 'test'.");
  }
}
