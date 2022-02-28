package com.google.cloud.bigquery.dwhassessment.extractiontool.executor;

import com.google.cloud.bigquery.dwhassessment.extractiontool.common.ChunkCheckpoint;
import com.google.common.collect.ImmutableMap;
import java.nio.file.Path;

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
}
