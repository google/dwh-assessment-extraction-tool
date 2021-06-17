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

import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilters.parse;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.re2j.Pattern;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class SchemaFiltersTest {

  @Test
  public void parse_empty_success() {
    assertThat(parse("")).isEqualTo(SchemaFilter.builder().build());
  }

  @Test
  public void parse_withTable_success() {
    assertThat(parse("table:foo"))
        .isEqualTo(SchemaFilter.builder().setTableName(Pattern.compile("foo")).build());
  }

  @Test
  public void parse_withTableAndDatabase_success() {
    assertThat(parse("db:bar,table:foo"))
        .isEqualTo(
            SchemaFilter.builder()
                .setTableName(Pattern.compile("foo"))
                .setDatabaseName(Pattern.compile("bar"))
                .build());
  }

  @Test
  public void parse_failureOnUnknownKey() {
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> parse("db:bar,table:foo,some:baz"));
    assertThat(e).hasMessageThat().contains("Got filter with invalid key(s): some.");
  }

  @Test
  public void parse_failureOnInvalidRegularExpression() {
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> parse("db:bar("));
    assertThat(e).hasMessageThat().contains("The pattern 'bar(' for filter key 'db' is invalid.");
  }
}
