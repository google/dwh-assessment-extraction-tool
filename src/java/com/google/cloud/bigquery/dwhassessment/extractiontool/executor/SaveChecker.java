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

import com.google.cloud.bigquery.dwhassessment.extractiontool.common.ChunkCheckpoint;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.nio.file.Path;
import java.util.Set;

/**
 * Interface to retrieve information used for selecting which scripts and time-ranges to query in
 * the new run based on previous run records
 */
public interface SaveChecker {

  /**
   * @param path The directory or file path containing records from the previous run(s).
   * @return The <scriptName, chunkCheckPoint> map indicating where the new run should start from.
   */
  ImmutableMap<String, ChunkCheckpoint> getScriptCheckPoints(Path path);

  /**
   * Given a set of scripts, returns the names of those that have finished successfully during a
   * previous run. Infers whether a script is finished from the names of the files present in the
   * target path.
   *
   * @param recordPath Target path containing records from the previous run(s).
   * @param scriptsToCheck The scripts whose records to look for.
   * @param fileExtension The extension of the record files (not including ".").
   * @return The set containing the names of the scripts.
   */
  ImmutableSet<String> getNamesOfFinishedScripts(
      Path recordPath, Set<String> scriptsToCheck, String fileExtension);
}
