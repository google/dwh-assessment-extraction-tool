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
