package com.google.cloud.bigquery.dwhassessment.extractiontool.db;

import static com.github.jknack.handlebars.TagType.VAR;
import static com.github.jknack.handlebars.Template.EMPTY;
import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.HandlebarsHelpers.getTableName;
import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.HandlebarsHelpers.whereClauseForQuerylogs;
import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.HandlebarsHelpers.whereClauseWithTimeRange;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertThrows;

import com.github.jknack.handlebars.Context;
import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Options;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SqlScriptVariables.QueryLogsVariables;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SqlScriptVariables.QueryLogsVariables.TimeRange;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class HandlebarsHelpersTest {
  private static final Object[] COMMAND_PARAMS = new Object[] {"testTableAlias", "testColumnName"};

  @Test
  public void whereClauseForQuerylogs_emptyQryLogVAlias_fail() {
    QueryLogsVariables queryLogsVariables = QueryLogsVariables.builder().build();
    Options options =
        getOptions(
            SqlScriptVariables.builder().setQueryLogsVariables(queryLogsVariables).build(),
            new Object[] {""});

    // Act
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> whereClauseForQuerylogs(queryLogsVariables, options));

    // Assert
    assertThat(e).hasMessageThat().isEqualTo("qryLogVAlias cannot be empty.");
  }

  @Test
  public void whereClauseForQuerylogs_noPassedParameter_fail() {
    QueryLogsVariables queryLogsVariables = QueryLogsVariables.builder().build();
    Options options =
        getOptions(
            SqlScriptVariables.builder().setQueryLogsVariables(queryLogsVariables).build(),
            new Object[] {});

    assertThrows(
        ArrayIndexOutOfBoundsException.class,
        () -> whereClauseForQuerylogs(queryLogsVariables, options));
  }

  @Test
  public void whereClauseForQuerylogs_noTimeRangeOrUsers_emptyResult() {
    QueryLogsVariables queryLogsVariables = QueryLogsVariables.builder().build();
    Options options =
        getOptions(
            SqlScriptVariables.builder().setQueryLogsVariables(queryLogsVariables).build(),
            COMMAND_PARAMS);

    // Act
    CharSequence result = whereClauseForQuerylogs(queryLogsVariables, options);

    // Assert
    assertThat(result).isEqualTo("");
  }

  @Test
  public void whereClauseForQuerylogs_onlyTimeRange_success() {
    QueryLogsVariables queryLogsVariables =
        QueryLogsVariables.builder()
            .setTimeRange(
                TimeRange.builder()
                    .setStartTimestamp("StartTimestamp")
                    .setEndTimestamp("EndTimeStamp")
                    .build())
            .build();
    Options options =
        getOptions(
            SqlScriptVariables.builder().setQueryLogsVariables(queryLogsVariables).build(),
            COMMAND_PARAMS);

    // Act
    CharSequence result = whereClauseForQuerylogs(queryLogsVariables, options);

    // Assert
    assertThat(result)
        .isEqualTo(
            "\nWHERE\n"
                + "\"testTableAlias\".\"StartTime\" BETWEEN TIMESTAMP 'StartTimestamp' AND"
                + " TIMESTAMP 'EndTimeStamp'");
  }

  @Test
  public void whereClauseForQuerylogs_onlyUsers_success() {
    QueryLogsVariables queryLogsVariables =
        QueryLogsVariables.builder().setUsers(ImmutableSet.of("user1", "user2")).build();
    Options options =
        getOptions(
            SqlScriptVariables.builder().setQueryLogsVariables(queryLogsVariables).build(),
            COMMAND_PARAMS);

    // Act
    CharSequence result = whereClauseForQuerylogs(queryLogsVariables, options);

    // Assert
    assertThat(result).isEqualTo("\nWHERE\n\"testTableAlias\".\"UserName\" IN ('user1','user2')");
  }

  @Test
  public void whereClauseForQuerylogs_timeRangeAndUsers_success() {
    QueryLogsVariables queryLogsVariables =
        QueryLogsVariables.builder()
            .setTimeRange(
                TimeRange.builder()
                    .setStartTimestamp("StartTimestamp")
                    .setEndTimestamp("EndTimeStamp")
                    .build())
            .setUsers(ImmutableSet.of("user1", "user2"))
            .build();
    Options options =
        getOptions(
            SqlScriptVariables.builder().setQueryLogsVariables(queryLogsVariables).build(),
            COMMAND_PARAMS);

    // Act
    CharSequence result = whereClauseForQuerylogs(queryLogsVariables, options);

    // Assert
    assertThat(result)
        .isEqualTo(
            "\nWHERE\n"
                + "\"testTableAlias\".\"StartTime\" BETWEEN TIMESTAMP 'StartTimestamp' AND"
                + " TIMESTAMP 'EndTimeStamp'"
                + "\nAND\n"
                + "\"testTableAlias\".\"UserName\" IN ('user1','user2')");
  }

  @Test
  public void whereClauseWithTimeRange_noPassedParameter_fail() {
    QueryLogsVariables queryLogsVariables = QueryLogsVariables.builder().build();
    Options options =
        getOptions(
            SqlScriptVariables.builder().setQueryLogsVariables(queryLogsVariables).build(),
            new Object[] {});

    assertThrows(
        ArrayIndexOutOfBoundsException.class,
        () -> whereClauseWithTimeRange(queryLogsVariables, options));
  }

  @Test
  public void whereClauseWithTimeRange_noTimeRange_emptyResult() {
    QueryLogsVariables queryLogsVariables = QueryLogsVariables.builder().build();
    Options options =
        getOptions(
            SqlScriptVariables.builder().setQueryLogsVariables(queryLogsVariables).build(),
            COMMAND_PARAMS);

    // Act
    CharSequence result = whereClauseWithTimeRange(queryLogsVariables, options);

    // Assert
    assertThat(result).isEqualTo("");
  }

  @Test
  public void whereClauseWithTimeRange_withTimeRange_success() {
    QueryLogsVariables queryLogsVariables =
        QueryLogsVariables.builder()
            .setTimeRange(
                TimeRange.builder()
                    .setStartTimestamp("StartTimestamp")
                    .setEndTimestamp("EndTimeStamp")
                    .build())
            .build();
    Options options =
        getOptions(
            SqlScriptVariables.builder().setQueryLogsVariables(queryLogsVariables).build(),
            COMMAND_PARAMS);

    // Act
    CharSequence result = whereClauseWithTimeRange(queryLogsVariables, options);

    // Assert
    assertThat(result)
        .isEqualTo(
            "WHERE \"testTableAlias\".\"testColumnName\" BETWEEN TIMESTAMP 'StartTimestamp' AND"
                + " TIMESTAMP 'EndTimeStamp'");
  }

  @Test
  public void whereClauseWithTimeRange_emptyTableAlias_fail() {
    QueryLogsVariables queryLogsVariables = QueryLogsVariables.builder().build();
    Options options =
        getOptions(
            SqlScriptVariables.builder().setQueryLogsVariables(queryLogsVariables).build(),
            new Object[] {"", "columnAlias"});

    // Act
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> whereClauseWithTimeRange(queryLogsVariables, options));

    // Assert
    assertThat(e).hasMessageThat().isEqualTo("tableAlias cannot be empty.");
  }

  @Test
  public void whereClauseWithTimeRange_emptyColumnAlias_fail() {
    QueryLogsVariables queryLogsVariables = QueryLogsVariables.builder().build();
    Options options =
        getOptions(
            SqlScriptVariables.builder().setQueryLogsVariables(queryLogsVariables).build(),
            new Object[] {"tableAlias", ""});

    // Act
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> whereClauseWithTimeRange(queryLogsVariables, options));

    // Assert
    assertThat(e).hasMessageThat().isEqualTo("columnAlias cannot be empty.");
  }

  @Test
  public void getTableName_noTableNameOverride_success() {
    Options options =
        getOptions(
            SqlScriptVariables.builder()
                .setBaseDatabase("testDB")
                .setQueryLogsVariables(QueryLogsVariables.builder().build())
                .build(),
            COMMAND_PARAMS);

    // Act
    CharSequence result = getTableName("testTableName", options);

    // Assert
    assertThat(result).isEqualTo("\"testDB\".\"testTableName\"");
  }

  @Test
  public void getTableName_withTableNameOverride_success() {
    Options options =
        getOptions(
            SqlScriptVariables.builder()
                .setBaseDatabase("testDB")
                .setVars(ImmutableMap.of("tableName", "nameOverride"))
                .setQueryLogsVariables(QueryLogsVariables.builder().build())
                .build(),
            new Object[] {});

    // Act
    CharSequence result = getTableName("testTableName", options);

    // Assert
    assertThat(result).isEqualTo("\"testDB\".\"nameOverride\"");
  }

  @Test
  public void getTableName_charactersEscape_success() {
    Options options =
        getOptions(
            SqlScriptVariables.builder()
                .setQueryLogsVariables(QueryLogsVariables.builder().build())
                .build(),
            new Object[] {});

    // Act
    CharSequence result = getTableName("\"testTableName\"", options);

    // Assert
    assertThat(result).isEqualTo("\"DBC\".\"&quot;testTableName&quot;\"");
  }

  @Test
  public void getTableName_noDatabaseOverride_success() {
    Options options =
        getOptions(
            SqlScriptVariables.builder()
                .setQueryLogsVariables(QueryLogsVariables.builder().build())
                .build(),
            new Object[] {});

    // Act
    CharSequence result = getTableName("testTableName", options);

    // Assert
    assertThat(result).isEqualTo("\"DBC\".\"testTableName\"");
  }

  private Options getOptions(SqlScriptVariables model, Object[] params) {
    Handlebars handlebars = mock(Handlebars.class);
    Context context = Context.newContext(model);

    return new Options.Builder(handlebars, "helperName", VAR, context, EMPTY)
        .setParams(params)
        .build();
  }
}
