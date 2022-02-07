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
import static com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptManagerImpl.getUtcTimeStringFromTimestamp;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManagerTempTestImpl;
import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManagerTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ScriptManagerImplTest {

  private final String baseScript = "SELECT * FROM TestTable";

  private final ImmutableMap<String, Supplier<String>> scriptsMap =
      ImmutableMap.of(
          "default",
          () -> baseScript,
          "default_chunked",
          () ->
              baseScript
                  + "{{#if sortingColumns}}\n"
                  + "ORDER BY {{#each sortingColumns}}{{this}}{{#unless"
                  + " @last}},{{/unless}}{{/each}} ASC NULLS FIRST\n"
                  + "{{/if}}");
  private final ImmutableMap<String, ImmutableList<String>> sortingColumnsMap =
      ImmutableMap.of("default_chunked", ImmutableList.of("TIMESTAMPS"));
  private final SqlTemplateRenderer sqlTemplateRenderer =
      new SqlTemplateRendererImpl(
          SqlScriptVariables.builder()
              .setBaseDatabase("test-db")
              .setQueryLogsVariables(SqlScriptVariables.QueryLogsVariables.builder().build()));
  private ScriptManager scriptManager;
  private DataEntityManager dataEntityManager;
  private ByteArrayOutputStream outputStream;
  private ScriptRunner scriptRunner;

  @Before
  public void setUp() {
    scriptRunner = new ScriptRunnerImpl();
    outputStream = new ByteArrayOutputStream();
    dataEntityManager = new DataEntityManagerTesting(outputStream);
  }

  @Test
  public void executeScript_simpleTable_success() throws Exception {
    scriptManager = new ScriptManagerImpl(scriptRunner, scriptsMap, sortingColumnsMap);
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:db_0");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute("CREATE Table TestTable (" + "ID INTEGER," + "NAME VARCHAR(100)" + ")");
    baseStmt.execute("INSERT INTO TestTable VALUES (0, 'name_0')");
    baseStmt.close();
    connection.commit();
    scriptManager.executeScript(
        connection, /*dryRun=*/ false, sqlTemplateRenderer, "default", dataEntityManager, 5000);

    String sqlScript = "SELECT * FROM TestTable";
    Schema testSchema = scriptRunner.extractSchema(connection, sqlScript, "default", "namespace");

    DatumReader<Record> datumReader = new GenericDatumReader<>();
    DataFileReader<Record> reader =
        new DataFileReader<>(new SeekableByteArrayInput(outputStream.toByteArray()), datumReader);
    assertThat(reader.next())
        .isEqualTo(new GenericRecordBuilder(testSchema).set("ID", 0).set("NAME", "name_0").build());
  }

  @Test
  public void executeScript_emptyTable_success() throws Exception {
    scriptManager = new ScriptManagerImpl(scriptRunner, scriptsMap, sortingColumnsMap);
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:db_1");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute("CREATE Table TestTable (" + "ID INTEGER," + "NAME VARCHAR(100)" + ")");
    baseStmt.close();
    connection.commit();
    scriptManager.executeScript(
        connection, /*dryRun=*/ false, sqlTemplateRenderer, "default", dataEntityManager, 5000);

    String sqlScript = "SELECT * FROM TestTable";
    Schema testSchema = scriptRunner.extractSchema(connection, sqlScript, "default", "namespace");

    DatumReader<Record> datumReader = new GenericDatumReader<>();
    DataFileReader<Record> reader =
        new DataFileReader<>(new SeekableByteArrayInput(outputStream.toByteArray()), datumReader);
    assertThrows(NoSuchElementException.class, reader::next);
  }

  @Test
  public void getAllScriptNames_fail() throws Exception {
    scriptManager = new ScriptManagerImpl(scriptRunner, scriptsMap, sortingColumnsMap);
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:db_2");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute("CREATE Table TestTable (" + "ID INTEGER," + "NAME VARCHAR(100)" + ")");
    baseStmt.execute("INSERT INTO TestTable VALUES (0, 'name_0')");
    baseStmt.close();
    connection.commit();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            scriptManager.executeScript(
                connection,
                /*dryRun=*/ false,
                sqlTemplateRenderer,
                "not_existing_script_name",
                dataEntityManager,
                5000));
  }

  @Test
  public void getAllScriptNames_success() {
    scriptManager = new ScriptManagerImpl(scriptRunner, scriptsMap, sortingColumnsMap);
    assertThat(scriptManager.getAllScriptNames())
        .isEqualTo(ImmutableSet.of("default", "default_chunked"));
  }

  @Test
  public void getScript_success() {
    scriptManager = new ScriptManagerImpl(scriptRunner, scriptsMap, sortingColumnsMap);
    assertThat(scriptManager.getScript(sqlTemplateRenderer, "default", ImmutableList.of()))
        .isEqualTo(baseScript);
  }

  @Test
  public void getScript_sortingRenderedOnlyWithSortingColumns_success() {
    scriptManager = new ScriptManagerImpl(scriptRunner, scriptsMap, sortingColumnsMap);
    String renderedWithSortingColumns =
        scriptManager.getScript(
            sqlTemplateRenderer, "default_chunked", ImmutableList.of("SORTING_COLUMN"));
    assertThat(renderedWithSortingColumns)
        .isEqualTo(baseScript + "\nORDER BY SORTING_COLUMN ASC NULLS FIRST\n");
    String renderedWithOutSortingColumns =
        scriptManager.getScript(sqlTemplateRenderer, "default_chunked", ImmutableList.of());
    assertThat(renderedWithOutSortingColumns).isEqualTo(baseScript);
  }

  @Test
  public void getScript_fail() {
    scriptManager = new ScriptManagerImpl(scriptRunner, scriptsMap, sortingColumnsMap);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            scriptManager.getScript(sqlTemplateRenderer, "not_available_name", ImmutableList.of()));
  }

  private DataFileReader<Record> getReaderFromAvroFile(Path filePath) throws IOException {
    DatumReader<Record> datumReader = new GenericDatumReader<>();
    return new DataFileReader<>(filePath.toFile(), datumReader);
  }

  private void assertRecordEqualsExpected(Record record, Integer id, String timestampUtc) {
    assertThat(record.get(0)).isEqualTo(id);
    assertThat(record.get(1)).isEqualTo(Instant.parse(timestampUtc).toEpochMilli());
  }

  @Test
  public void executeScript_writeChunked_success() throws Exception {
    scriptManager = new ScriptManagerImpl(scriptRunner, scriptsMap, sortingColumnsMap);
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:db_3");
    Statement baseStmt = connection.createStatement();
    baseStmt.execute(
        "CREATE Table TestTable ("
            + "ID INTEGER,"
            + "TIMESTAMPS TIMESTAMP(6) WITH TIME ZONE"
            + ")");
    // Insert in different orders to test that the later query does apply ORDER BY.
    for (int i = 16; i > 10; i--) {
      baseStmt.execute(
          String.format(
              "INSERT INTO TestTable VALUES (%d, TIMESTAMP '2008-08-08 20:08:%02d.007000'"
                  + " AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE)",
              i, i + 8));
    }
    for (int i = 0; i < 11; i++) {
      baseStmt.execute(
          String.format(
              "INSERT INTO TestTable VALUES (%d, TIMESTAMP '2008-08-08 20:08:%02d.007000'"
                  + " AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE)",
              i, i + 8));
    }
    baseStmt.close();
    connection.commit();
    ImmutableList<String> expectedFiles =
        ImmutableList.<String>builder()
            .add("default_chunked-20080808T200808S007000-20080808T200810S007000_0.avro")
            .add("default_chunked-20080808T200811S007000-20080808T200813S007000_1.avro")
            .add("default_chunked-20080808T200814S007000-20080808T200816S007000_2.avro")
            .add("default_chunked-20080808T200817S007000-20080808T200819S007000_3.avro")
            .add("default_chunked-20080808T200820S007000-20080808T200822S007000_4.avro")
            .add("default_chunked-20080808T200823S007000-20080808T200824S007000_5.avro")
            .build();
    DataEntityManager dataEntityManagerTmp = new DataEntityManagerTempTestImpl("tmpTest");

    scriptManager.executeScript(
        connection,
        /*dryRun=*/ false,
        sqlTemplateRenderer,
        "default_chunked",
        dataEntityManagerTmp,
        3);

    assertThat(
            Files.walk(dataEntityManagerTmp.getAbsolutePath(""))
                .filter(Files::isRegularFile)
                .sorted()
                .map(path -> path.getFileName().toString())
                .collect(Collectors.toList()))
        .isEqualTo(expectedFiles);
    // Validate result details for the first and the last chunks.
    DataFileReader<Record> reader;
    reader =
        getReaderFromAvroFile(
            dataEntityManagerTmp.getAbsolutePath(
                "default_chunked-20080808T200808S007000-20080808T200810S007000_0.avro"));
    assertRecordEqualsExpected(reader.next(), 0, "2008-08-08T20:08:08.007000000Z");
    assertRecordEqualsExpected(reader.next(), 1, "2008-08-08T20:08:09.007000000Z");
    assertRecordEqualsExpected(reader.next(), 2, "2008-08-08T20:08:10.007000000Z");
    assertFalse(reader.hasNext());
    reader =
        getReaderFromAvroFile(
            dataEntityManagerTmp.getAbsolutePath(
                "default_chunked-20080808T200823S007000-20080808T200824S007000_5.avro"));
    assertRecordEqualsExpected(reader.next(), 15, "2008-08-08T20:08:23.007000000Z");
    assertRecordEqualsExpected(reader.next(), 16, "2008-08-08T20:08:24.007000000Z");
    assertFalse(reader.hasNext());
  }

  @Test
  public void getUtcTimeStringFromTimestamp_outputShouldBeCorrect() {
    assertThat(getUtcTimeStringFromTimestamp(Timestamp.from(Instant.parse("2022-01-24T14:52:00Z"))))
        .isEqualTo("20220124T145200S000000");
    assertThat(
            getUtcTimeStringFromTimestamp(
                Timestamp.from(Instant.parse("2022-01-24T14:52:00.000Z"))))
        .isEqualTo("20220124T145200S000000");
    assertThat(
            getUtcTimeStringFromTimestamp(
                Timestamp.from(Instant.parse("2022-01-24T14:52:00.123456Z"))))
        .isEqualTo("20220124T145200S123456");
    assertThat(
            getUtcTimeStringFromTimestamp(
                Timestamp.from(Instant.parse("2022-01-24T14:52:00.123Z"))))
        .isEqualTo("20220124T145200S123000");
    assertThat(
            getUtcTimeStringFromTimestamp(
                Timestamp.from(Instant.parse("2022-01-24T14:52:00.123456789Z"))))
        .isEqualTo("20220124T145200S123457");
  }
}
