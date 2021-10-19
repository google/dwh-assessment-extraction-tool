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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SqlTemplateRendererTest {

  private SqlTemplateRenderer underTest;

  @Before
  public void setUp() {
    underTest =
        new SqlTemplateRendererImpl(
            SqlScriptVariables.builder().setBaseDatabase("test-db").build());
  }

  @Test
  public void render_success() {
    assertThat(
            underTest.renderTemplate(
                /* name= */ "test",
                /* sql= */ "SELECT t.a AS b FROM \"{{baseDatabase}}\".bar AS t"))
        .isEqualTo("SELECT t.a AS b FROM \"test-db\".bar AS t");
  }

  @Test
  public void render_invalidTemplateThrows() {
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> underTest.renderTemplate(/* name= */ "test", /* sql= */ "{{#foo}}"));
    assertThat(e).hasMessageThat().contains("Failed to compile SQL template 'test'.");
  }
}
