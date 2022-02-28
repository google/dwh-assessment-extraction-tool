package com.google.cloud.bigquery.dwhassessment.extractiontool.executor;

import com.google.cloud.bigquery.dwhassessment.extractiontool.common.ChunkCheckpoint;
import com.google.common.collect.ImmutableMap;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

public interface SaveChecker {

  ImmutableMap<String, ChunkCheckpoint> getScriptCheckPoints(Path path);
}
