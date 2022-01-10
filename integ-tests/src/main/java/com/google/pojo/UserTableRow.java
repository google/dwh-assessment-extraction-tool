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

import java.util.Objects;

/** POJO class for serialization data from DB and Avro files. */
public class UserTableRow {

  public UserName userName = new UserName();
  public CreatorName creatorName = new CreatorName();
  public CreateTimeStamp createTimeStamp = new CreateTimeStamp();
  public LastAccessTimeStamp lastAccessTimeStamp = new LastAccessTimeStamp();

  @Override
  public String toString() {
    return "{"
        + "UserName: {"
        + userName.toString()
        + "}"
        + ", CreatorName: {"
        + creatorName.toString()
        + "}"
        + ", CreateTimeStamp: {"
        + createTimeStamp.toString()
        + "}"
        + ", LastAccessTimeStamp: {"
        + lastAccessTimeStamp.toString()
        + "}"
        + "}\n";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof UserTableRow)) return false;
    UserTableRow that = (UserTableRow) o;
    return Objects.equals(userName, that.userName)
        && Objects.equals(creatorName, that.creatorName)
        && Objects.equals(createTimeStamp, that.createTimeStamp)
        && Objects.equals(lastAccessTimeStamp, that.lastAccessTimeStamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userName, creatorName, createTimeStamp, lastAccessTimeStamp);
  }

  /** Inner POJO class for serialization data from DB and Avro files. */
  public static class UserName {
    public String name;

    @Override
    public String toString() {
      return "string: " + name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof UserName)) return false;
      UserName userName = (UserName) o;
      return Objects.equals(name, userName.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }
  }

  /** Inner POJO class for serialization data from DB and Avro files. */
  public static class CreatorName {
    public String name;

    @Override
    public String toString() {
      return "string: " + name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof CreatorName)) return false;
      CreatorName that = (CreatorName) o;
      return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }
  }

  /** Inner POJO class for serialization data from DB and Avro files. */
  public static class CreateTimeStamp {
    public long timestamp;

    @Override
    public String toString() {
      return "long: " + timestamp;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof CreateTimeStamp)) return false;
      CreateTimeStamp that = (CreateTimeStamp) o;
      return timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
      return Objects.hash(timestamp);
    }
  }

  /** Inner POJO class for serialization data from DB and Avro files. */
  public static class LastAccessTimeStamp {
    public long timestamp;

    @Override
    public String toString() {
      return "long: " + timestamp;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof LastAccessTimeStamp)) return false;
      LastAccessTimeStamp that = (LastAccessTimeStamp) o;
      return timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
      return Objects.hash(timestamp);
    }
  }
}
