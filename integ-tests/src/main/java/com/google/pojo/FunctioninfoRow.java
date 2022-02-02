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
 * POJO class for serialization data from DB and Avro files.
 */
@AutoValue
public abstract class FunctioninfoRow {

  public abstract String databaseName();

  public abstract String functionName();

  public abstract String specificName();

  public abstract byte[] functionId();

  public abstract int numParameters();

  public abstract String parameterDataTypes();

  public abstract String functionType();

  public abstract String externalName();

  public abstract String srcFileLanguage();

  public abstract String noSQLDataAccess();

  public abstract String parameterStyle();

  public abstract String deterministicOpt();

  public abstract String nullCall();

  public abstract String prepareCount();

  public abstract String execProtectionMode();

  public abstract String extFileReference();

  public abstract int characterType();

  public abstract String platform();

  public abstract int interimFldSize();

  public abstract String routineKind();

  public abstract byte[] parameterUDTIds();

  public abstract int maxOutParameters();

  public abstract String glopSetDatabaseName();

  public abstract String glopSetMemberName();

  public abstract String refQueryband();

  public abstract String execMapName();

  public abstract String execMapColocName();

  @Override
  public String toString() {
    return "{"
        + "databaseName=" + databaseName()
        + ", functionName=" + functionName()
        + ", specificName=" + specificName()
        + ", functionId=" + functionId()
        + ", numParameters=" + numParameters()
        + ", parameterDataTypes=" + parameterDataTypes()
        + ", functionType=" + functionType()
        + ", externalName=" + externalName()
        + ", srcFileLanguage=" + srcFileLanguage()
        + ", noSQLDataAccess=" + noSQLDataAccess()
        + ", parameterStyle=" + parameterStyle()
        + ", deterministicOpt=" + deterministicOpt()
        + ", nullCall=" + nullCall()
        + ", prepareCount=" + prepareCount()
        + ", execProtectionMode=" + execProtectionMode()
        + ", extFileReference=" + extFileReference()
        + ", characterType=" + characterType()
        + ", platform=" + platform()
        + ", interimFldSize=" + interimFldSize()
        + ", routineKind=" + routineKind()
        + ", parameterUDTIds=" + parameterUDTIds()
        + ", maxOutParameters=" + maxOutParameters()
        + ", glopSetDatabaseName=" + glopSetDatabaseName()
        + ", glopSetMemberName=" + glopSetMemberName()
        + ", refQueryband=" + refQueryband()
        + ", execMapName=" + execMapName()
        + ", execMapColocName=" + execMapColocName()
        + "}\n";
  }

  public static FunctioninfoRow create(String databaseName, String functionName,
      String specificName, byte[] functionId, int numParameters, String parameterDataTypes,
      String functionType,
      String externalName, String srcFileLanguage, String noSQLDataAccess, String parameterStyle,
      String deterministicOpt, String nullCall, String prepareCount, String execProtectionMode,
      String extFileReference, int characterType, String platform, int interimFldSize,
      String routineKind, byte[] parameterUDTIds, int maxOutParameters, String glopSetDatabaseName,
      String glopSetMemberName, String refQueryband, String execMapName, String execMapColocName) {
    return new AutoValue_FunctioninfoRow(databaseName, functionName, specificName, functionId,
        numParameters, parameterDataTypes, functionType, externalName, srcFileLanguage,
        noSQLDataAccess, parameterStyle, deterministicOpt, nullCall, prepareCount,
        execProtectionMode, extFileReference, characterType, platform, interimFldSize, routineKind,
        parameterUDTIds, maxOutParameters, glopSetDatabaseName, glopSetMemberName, refQueryband,
        execMapName, execMapColocName);
  }
}