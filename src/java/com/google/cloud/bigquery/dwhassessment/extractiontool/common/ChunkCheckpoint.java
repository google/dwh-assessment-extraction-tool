package com.google.cloud.bigquery.dwhassessment.extractiontool.common;

import com.google.auto.value.AutoValue;
import java.time.Instant;

@AutoValue
public abstract class ChunkCheckpoint {

  public abstract Integer lastSavedChunkNumber();

  public abstract Instant lastSavedInstant();

  public static Builder builder() {
    return new AutoValue_ChunkCheckpoint.Builder()
        .setLastSavedChunkNumber(-1)
        .setLastSavedInstant(Instant.ofEpochMilli(0));
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setLastSavedChunkNumber(Integer value);

    public abstract Builder setLastSavedInstant(Instant value);

    public abstract ChunkCheckpoint build();
  }
}
