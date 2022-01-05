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

/** POJO class for serialization data from DB and Avro files. */
public class UserTableRow {

  public UserName userName = new UserName();
  public CreatorName creatorName = new CreatorName();
  public CreateTimeStamp createTimeStamp = new CreateTimeStamp();
  public LastAccessTimeStamp lastAccessTimeStamp = new LastAccessTimeStamp();

  @Override
  public String toString() {
    return "UserTableRow: {"
        + "userName: {"
        + userName.toString()
        + "}"
        + ", creatorName: {"
        + creatorName.toString()
        + "}"
        + ", createTimeStamp: {"
        + createTimeStamp.toString()
        + "}"
        + ", lastAccessTimeStamp: {"
        + lastAccessTimeStamp.toString()
        + "}"
        + '}';
  }

  /** Inner POJO class for serialization data from DB and Avro files. */
  public class UserName {
    public String name;

    @Override
    public String toString() {
      return name.getClass().getSimpleName() + ": " + name;
    }
  }

  /** Inner POJO class for serialization data from DB and Avro files. */
  public class CreatorName {
    public String name;

    @Override
    public String toString() {
      return name.getClass().getSimpleName() + ": " + name;
    }
  }

  /** Inner POJO class for serialization data from DB and Avro files. */
  public class CreateTimeStamp {
    public long timestamp;

    @Override
    public String toString() {
      return Long.class.getSimpleName() + ": " + timestamp;
    }
  }

  /** Inner POJO class for serialization data from DB and Avro files. */
  public class LastAccessTimeStamp {
    public long timestamp;

    @Override
    public String toString() {
      return Long.class.getSimpleName() + ": " + timestamp;
    }
  }
}
