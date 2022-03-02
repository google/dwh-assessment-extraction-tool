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
package com.google.cloud.bigquery.dwhassessment.extractiontool.executor;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.dwhassessment.extractiontool.common.ChunkCheckpoint;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SaveCheckerImplTest {
  private Path tmpDir;
  private SaveCheckerImpl saveChecker;
  private static final String DIR_NAME = "testDir";
  private static final String SCRIPT_NAME = "test_script";
  private static final String AVRO_SUFFIX = ".avro";

  @Before
  public void setUp() throws IOException {
    tmpDir = Files.createTempDirectory(DIR_NAME);
    saveChecker =
        new SaveCheckerImpl(ImmutableMap.of(SCRIPT_NAME, ImmutableList.of("testTimestampColumn")));
  }

  @Test
  public void getScriptCheckpoints_success() throws IOException {
    Files.createFile(
        tmpDir.resolve(
            SCRIPT_NAME + "-20140707T170707S000007-20140707T170707S000008_0" + AVRO_SUFFIX));
    Files.createFile(
        tmpDir.resolve(
            SCRIPT_NAME + "-20140707T170707S000017-20140707T170707S000018_1" + AVRO_SUFFIX));
    Files.createFile(
        tmpDir.resolve(
            SCRIPT_NAME + "-20140707T170707S000027-20140707T170707S000028_2" + AVRO_SUFFIX));
    Files.createFile(tmpDir.resolve("file_to_be_ignored"));

    ImmutableMap<String, ChunkCheckpoint> checkpoints = saveChecker.getScriptCheckPoints(tmpDir);

    assertThat(checkpoints)
        .isEqualTo(
            ImmutableMap.of(
                SCRIPT_NAME,
                ChunkCheckpoint.builder()
                    .setLastSavedChunkNumber(2)
                    .setLastSavedInstant(Instant.parse("2014-07-07T17:07:07.000028Z"))
                    .build()));
  }

  @Test
  public void getScriptCheckpoints_unmatchingFilenamesAreIgnored() throws IOException {
    // Lower cased timestamp separators.
    Files.createFile(
        tmpDir.resolve(
            SCRIPT_NAME + "-20140707t170707s000007-20140707t170707s000008_0" + AVRO_SUFFIX));
    // Non-avro suffix.
    Files.createFile(
        tmpDir.resolve(SCRIPT_NAME + "-20140707T170707S000027-20140707T170707S000028_0" + ".txt"));
    // Directory.
    Files.createDirectory(
        tmpDir.resolve(
            SCRIPT_NAME + "-20140707T170707S000047-20140707T170707S000048_0" + AVRO_SUFFIX));
    // Non-chunked file.
    Files.createDirectory(tmpDir.resolve(SCRIPT_NAME + AVRO_SUFFIX));

    ImmutableMap<String, ChunkCheckpoint> checkpoints = saveChecker.getScriptCheckPoints(tmpDir);

    assertThat(checkpoints).isEqualTo(ImmutableMap.of());
  }

  @Test
  public void getScriptCheckpoints_successWithEdgeCaseTimestamp() throws IOException {
    Files.createFile(
        tmpDir.resolve(
            SCRIPT_NAME + "-00000101T000000S000000-00000101T000000S000000_0" + AVRO_SUFFIX));
    ImmutableMap<String, ChunkCheckpoint> checkpoints = saveChecker.getScriptCheckPoints(tmpDir);

    assertThat(checkpoints)
        .isEqualTo(
            ImmutableMap.of(
                SCRIPT_NAME,
                ChunkCheckpoint.builder()
                    .setLastSavedChunkNumber(0)
                    .setLastSavedInstant(Instant.parse("0000-01-01T00:00:00.000000Z"))
                    .build()));
  }

  @Test
  public void getScriptCheckpoints_unparsableLastTimestamp_throwsException() throws IOException {
    Files.createFile(
        tmpDir.resolve(
            SCRIPT_NAME + "-00000000T000000S000000-00000000T000000S000000_0" + AVRO_SUFFIX));

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> saveChecker.getScriptCheckPoints(tmpDir));
    assertThat(e).hasMessageThat().contains("Error parsing the last timestamp");
  }

  @Test
  public void getScriptCheckpoints_successAsLongAsUnparsableTimestampIsNotInTheLastChunk()
      throws IOException {
    Files.createFile(
        tmpDir.resolve(
            SCRIPT_NAME + "-00000000T000000S000000-00000000T000000S000000_0" + AVRO_SUFFIX));
    Files.createFile(
        tmpDir.resolve(
            SCRIPT_NAME + "-00000000T000001S000000-00000101T000000S000000_1" + AVRO_SUFFIX));

    ImmutableMap<String, ChunkCheckpoint> checkpoints = saveChecker.getScriptCheckPoints(tmpDir);
    assertThat(checkpoints)
        .isEqualTo(
            ImmutableMap.of(
                SCRIPT_NAME,
                ChunkCheckpoint.builder()
                    .setLastSavedChunkNumber(1)
                    .setLastSavedInstant(Instant.parse("0000-01-01T00:00:00.000000Z"))
                    .build()));
  }

  @Test
  public void getScriptCheckpoints_nonConsecutiveChunkNumbers_throwsException() throws IOException {
    Files.createFile(
        tmpDir.resolve(
            SCRIPT_NAME + "-20140707T170707S000007-20140707T170707S000008_0" + AVRO_SUFFIX));
    Files.createFile(
        tmpDir.resolve(
            SCRIPT_NAME + "-20140707T170707S000027-20140707T170707S000028_2" + AVRO_SUFFIX));

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> saveChecker.getScriptCheckPoints(tmpDir));
    assertThat(e).hasMessageThat().contains("consecutiveness");
  }

  @Test
  public void getScriptCheckpoints_initialChunkNumberLargerThanZero_throwsException()
      throws IOException {
    Files.createFile(
        tmpDir.resolve(
            SCRIPT_NAME + "-20140707T170707S000007-20140707T170707S000017_1" + AVRO_SUFFIX));

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> saveChecker.getScriptCheckPoints(tmpDir));
    assertThat(e).hasMessageThat().contains("consecutiveness");
  }

  @Test
  public void getScriptCheckpoints_overlappingTimestamps_throwsException() throws IOException {
    Files.createFile(
        tmpDir.resolve(
            SCRIPT_NAME + "-20140707T170707S000007-20140707T170707S000028_0" + AVRO_SUFFIX));
    Files.createFile(
        tmpDir.resolve(
            SCRIPT_NAME + "-20140707T170707S000017-20140707T170707S000028_1" + AVRO_SUFFIX));

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> saveChecker.getScriptCheckPoints(tmpDir));
    assertThat(e)
        .hasMessageThat()
        .contains("no later than the last time stamp of the previous file");
  }

  @Test
  public void getScriptCheckpoints_reversedTimestamps_throwsException() throws IOException {
    Files.createFile(
        tmpDir.resolve(
            SCRIPT_NAME + "-20140707T170707S000017-20140707T170707S000007_0" + AVRO_SUFFIX));

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> saveChecker.getScriptCheckPoints(tmpDir));
    assertThat(e).hasMessageThat().contains("earlier than the first one");
  }
}
