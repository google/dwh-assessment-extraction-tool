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
package com.google.pojo;

import com.google.auto.value.AutoValue;

/**
 * AutoValue abstract POJO class for serialization data from DB and Avro files.
 */
@AutoValue
public abstract class DiskspaceRow {

  public abstract int vproc();

  public abstract String databaseName();

  public abstract String accountName();

  public abstract long maxPerm();

  public abstract long maxSpool();

  public abstract long maxTemp();

  public abstract long currentSpool();

  public abstract long currentPersistentSpool();

  public abstract long currentTemp();

  public abstract long peakSpool();

  public abstract long peakPersistentSpool();

  public abstract long peakTemp();

  public abstract long maxProfileSpool();

  public abstract long maxProfileTemp();

  public abstract long allocatedPerm();

  public abstract long allocatedSpool();

  public abstract long allocatedTemp();

  public abstract int permSkew();

  public abstract int spoolSkew();

  public abstract int tempSkew();


  @Override
  public String toString() {
    return "{"
        + "Vproc=" + vproc()
        + ", DatabaseName=" + databaseName()
        + ", AccountName=" + accountName()
        + ", MaxPerm=" + maxPerm()
        + ", MaxSpool=" + maxSpool()
        + ", MaxTemp=" + maxTemp()
        + ", CurrentSpool=" + currentSpool()
        + ", CurrentPersistentSpool=" + currentPersistentSpool()
        + ", CurrentTemp=" + currentTemp()
        + ", PeakSpool=" + peakSpool()
        + ", PeakPersistentSpool=" + peakPersistentSpool()
        + ", PeakTemp=" + peakTemp()
        + ", MaxProfileSpool=" + maxProfileSpool()
        + ", MaxProfileTemp=" + maxProfileTemp()
        + ", AllocatedPerm=" + allocatedPerm()
        + ", AllocatedSpool=" + allocatedSpool()
        + ", AllocatedTemp=" + allocatedTemp()
        + ", PermSkew=" + permSkew()
        + ", SpoolSkew=" + spoolSkew()
        + ", TempSkew=" + tempSkew()
        + "}\n";
  }

  public static DiskspaceRow create(int newVproc, String newDatabaseName, String newAccountName,
      long newMaxPerm, long newMaxSpool, long newMaxTemp, long newCurrentSpool,
      long newCurrentPersistentSpool, long newCurrentTemp, long newPeakSpool,
      long newPeakPersistentSpool, long newPeakTemp, long newMaxProfileSpool,
      long newMaxProfileTemp, long newAllocatedPerm, long newAllocatedSpool, long newAllocatedTemp,
      int newPermSkew, int newSpoolSkew, int newTempSkew) {
    return new AutoValue_DiskspaceRow(newVproc, newDatabaseName, newAccountName, newMaxPerm,
        newMaxSpool, newMaxTemp, newCurrentSpool, newCurrentPersistentSpool,
        newCurrentTemp, newPeakSpool, newPeakPersistentSpool, newPeakTemp,
        newMaxProfileSpool, newMaxProfileTemp, newAllocatedPerm, newAllocatedSpool,
        newAllocatedTemp, newPermSkew, newSpoolSkew, newTempSkew);
  }
}