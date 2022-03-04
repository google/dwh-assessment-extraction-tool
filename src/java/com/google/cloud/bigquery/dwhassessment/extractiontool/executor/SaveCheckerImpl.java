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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.collectingAndThen;

import com.google.cloud.bigquery.dwhassessment.extractiontool.common.ChunkCheckpoint;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

public class SaveCheckerImpl implements SaveChecker {

  private final ImmutableMap<String, ImmutableList<String>> sortingColumnsMap;

  // The expected filename format is
  // "input_type-yyyymmddThhmmssSffffff-yyyymmddThhmmssSffffff_n.avro",
  // where "input_type" is one of the assessment avro files, the two timestamps are the first and
  // last timestamps, and “n” is the index of the chunk,
  // respectively. See go/chunked-dwh-assessment-extraction-dd for further details.
  private static final Pattern INPUT_CHUNK_PATTERN =
      Pattern.compile(
          "(?P<scriptName>[\\w_]+)-(\\d{8}T\\d{6}S\\d{6})-(\\d{8}T\\d{6}S\\d{6})_(?P<chunkNumber>\\d+)\\.avro");

  // TODO(cyulysses-corp): collect different date-time parser/formatters into the same place for
  // reliable references.
  private static final DateTimeFormatter chunkTimestampFormatter =
      new DateTimeFormatterBuilder()
          .parseCaseSensitive()
          .append(DateTimeFormatter.BASIC_ISO_DATE)
          .appendLiteral("T")
          .append(DateTimeFormatter.ofPattern("HHmmss"))
          .appendLiteral("S")
          .append(DateTimeFormatter.ofPattern("SSSSSS"))
          .toFormatter(Locale.ENGLISH);

  public SaveCheckerImpl(ImmutableMap<String, ImmutableList<String>> sortingColumnsMap) {
    this.sortingColumnsMap = sortingColumnsMap;
  }

  private static Map<String, List<Matcher>> getFileMapSortingEachGroupByChunkNumber(Path path) {
    try {
      return Files.walk(path)
          .filter(Files::isRegularFile)
          .map(oneFile -> INPUT_CHUNK_PATTERN.matcher(oneFile.getFileName().toString()))
          .filter(Matcher::matches)
          .collect(
              groupingBy(
                  (Matcher matcher) -> matcher.group("scriptName"),
                  mapping(
                      Function.identity(),
                      collectingAndThen(
                          toList(),
                          list ->
                              list.stream()
                                  .sorted(
                                      Comparator.comparingInt(
                                          matcher ->
                                              Integer.parseInt(matcher.group("chunkNumber"))))
                                  .collect(toList())))));
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format("Error reading path '%s'.", path.toString()), e);
    }
  }

  private static Instant getInstantFromFilenameTimestamp(String timestamp) {
    return ZonedDateTime.of(
            chunkTimestampFormatter.parse(timestamp, LocalDateTime::from), ZoneOffset.UTC)
        .toInstant();
  }

  @Override
  public ImmutableMap<String, ChunkCheckpoint> getScriptCheckPoints(Path path) {
    Map<String, List<Matcher>> fileMap = getFileMapSortingEachGroupByChunkNumber(path);
    ImmutableMap.Builder<String, ChunkCheckpoint> checkPointsMapBuilder = ImmutableMap.builder();
    for (String scriptName : sortingColumnsMap.keySet()) {
      if (fileMap.containsKey(scriptName) && !fileMap.get(scriptName).isEmpty()) {
        List<Matcher> matchers = fileMap.get(scriptName);
        // Note that the validator only validates information contained in the file names.
        // The timestamp comparisons are only done lexicographically without checking whether each
        // timestamp can be parsed. This is because the probability of having incorrectly generated
        // timestamps in filenames while other aspects are normal is very low.
        validateSortedChunkSequence(matchers);
        Matcher lastMatcher = Iterables.getLast(matchers);
        Instant lastInstant;
        String lastTimestamp = lastMatcher.group(3);
        try {
          lastInstant = getInstantFromFilenameTimestamp(lastTimestamp);
        } catch (DateTimeParseException e) {
          throw new IllegalStateException(
              String.format(
                  "Error parsing the last timestamp in the file name of the last chunk %s.",
                  lastMatcher.group()),
              e);
        }
        checkPointsMapBuilder.put(
            scriptName,
            ChunkCheckpoint.builder()
                .setLastSavedInstant(lastInstant)
                .setLastSavedChunkNumber(Integer.parseInt(lastMatcher.group("chunkNumber")))
                .build());
      }
    }
    return checkPointsMapBuilder.build();
  }

  private static void validateSortedChunkSequence(List<Matcher> matchers) {
    String prevEnding = "-10000000T000000S000000";
    int prevChunkIndex = -1;
    String prevFileName = "(beginning)";
    String fileName, firstTimestamp, lastTimestamp, chunkIndex;
    for (Matcher matcher : matchers) {
      fileName = matcher.group();
      firstTimestamp = matcher.group(2);
      lastTimestamp = matcher.group(3);
      chunkIndex = matcher.group(4);

      // Check that the chunk numbers of two consecutive chunk are also consecutive.
      if (Integer.parseInt(chunkIndex) - prevChunkIndex != 1) {
        throw new IllegalStateException(
            String.format(
                "The chunk index of file %s breaks the consecutiveness with other files (the"
                    + " previous chunk number is %d), possibly indicating missing files."
                    + " Aborting.",
                fileName, prevChunkIndex));
      }
      // Check that there is no overlap between the timeranges of two consecutive chunks.
      if (firstTimestamp.compareTo(prevEnding) <= 0) {
        throw new IllegalStateException(
            String.format(
                "The first time stamp of file %s is no later than the last time stamp of the"
                    + " previous file %s, possibly indicating mixed runs. Aborting. ",
                fileName, prevFileName));
      }
      // Check that the timestamps in the same filename are not in reversed order.
      if (lastTimestamp.compareTo(firstTimestamp) < 0) {
        throw new IllegalStateException(
            String.format(
                "The last timestamp in file %s is earlier than the first one, which should not"
                    + " happen. Aborting.",
                fileName));
      }

      prevChunkIndex++;
      prevFileName = fileName;
      prevEnding = lastTimestamp;
    }
  }
}
