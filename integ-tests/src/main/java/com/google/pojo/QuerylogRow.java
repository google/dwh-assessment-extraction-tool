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


import static java.lang.System.lineSeparator;

import com.google.auto.value.AutoValue;
import java.util.Arrays;

/**
 * POJO class for serialization data from DB and Avro files.
 */
@AutoValue
public abstract class QuerylogRow {

  public abstract String abortFlag();
  public abstract String acctString();
  public abstract long acctStringDate();
  public abstract int acctStringHour();
  public abstract double acctStringTime();
  public abstract double aMPCPUTime();
  public abstract double aMPCPUTimeNorm();
  public abstract String appID();
  public abstract String cacheFlag();
  public abstract String calendarName();
  public abstract int callNestingLevel();
  public abstract double checkpointNum();
  public abstract String clientAddr();
  public abstract String clientID();
  public abstract long collectTimeStamp();
  public abstract int cPUDecayLevel();
  public abstract int dataCollectAlg();
  public abstract int dBQLStatus();
  public abstract String defaultDatabase();
  public abstract double delayTime();
  public abstract double disCPUTime();
  public abstract double disCPUTimeNorm();
  public abstract int errorCode();
  public abstract double estMaxRowCount();
  public abstract double estMaxStepTime();
  public abstract double estProcTime();
  public abstract double estResultRows();
  public abstract String expandAcctString();
  public abstract long firstRespTime();
  public abstract long firstStepTime();
  public abstract String flexThrottle();
  public abstract int impactSpool();
  public abstract int internalRequestNum();
  public abstract int iODecayLevel();
  public abstract int iterationCount();
  public abstract String keepFlag();
  public abstract long lastRespTime();
  public abstract double lockDelay();
  public abstract String lockLevel();
  public abstract int logicalHostID();
  public abstract long logonDateTime();
  public abstract String logonSource();
  public abstract double lSN();
  public abstract double maxAMPCPUTime();
  public abstract double maxAMPCPUTimeNorm();
  public abstract double maxAmpIO();
  public abstract int maxCPUAmpNumber();
  public abstract int maxCPUAmpNumberNorm();
  public abstract int maxIOAmpNumber();
  public abstract int maxNumMapAMPs();
  public abstract int maxOneMBRowSize();
  public abstract double maxStepMemory();
  public abstract int maxStepsInPar();
  public abstract double minAmpCPUTime();
  public abstract double minAmpCPUTimeNorm();
  public abstract double minAmpIO();
  public abstract int minNumMapAMPs();
  public abstract double minRespHoldTime();
  public abstract int numFragments();
  public abstract int numOfActiveAMPs();
  public abstract int numRequestCtx();
  public abstract double numResultOneMBRows();
  public abstract double numResultRows();
  public abstract int numSteps();
  public abstract int numStepswPar();
  public abstract String paramQuery();
  public abstract double parserCPUTime();
  public abstract double parserCPUTimeNorm();
  public abstract double parserExpReq();
  public abstract long persistentSpool();
  public abstract byte[] procID();
  public abstract String profileID();
  public abstract String profileName();
  public abstract String proxyRole();
  public abstract String proxyUser();
  public abstract String proxyUserID();
  public abstract String queryBand();
  public abstract byte[] queryID();
  public abstract String queryRedriven();
  public abstract String queryText();
  public abstract String reDriveKind();
  public abstract String remoteQuery();
  public abstract double reqIOKB();
  public abstract double reqPhysIO();
  public abstract double reqPhysIOKB();
  public abstract String requestMode();
  public abstract int requestNum();
  public abstract double seqRespTime();
  public abstract int sessionID();
  public abstract String sessionTemporalQualifier();
  public abstract long spoolUsage();
  public abstract long startTime();
  public abstract String statementGroup();
  public abstract int statements();
  public abstract String statementType();
  public abstract int sysDefNumMapAMPs();
  public abstract int tacticalCPUException();
  public abstract int tacticalIOException();
  public abstract double tDWMEstMemUsage();
  public abstract double throttleBypassed();
  public abstract double totalFirstRespTime();
  public abstract double totalIOCount();
  public abstract long totalServerByteCount();
  public abstract String tTGranularity();
  public abstract String txnMode();
  public abstract String txnUniq();
  public abstract String unitySQL();
  public abstract double unityTime();
  public abstract double usedIota();
  public abstract byte[] userID();
  public abstract String userName();
  public abstract int utilityByteCount();
  public abstract String utilityInfoAvailable();
  public abstract double utilityRowCount();
  public abstract double vHLogicalIO();
  public abstract double vHLogicalIOKB();
  public abstract double vHPhysIO();
  public abstract double vHPhysIOKB();
  public abstract String warningOnly();
  public abstract String wDName();

  @Override
  public String toString() {
    return "{"
        + "abortFlag=" + abortFlag()
        + ", acctString=" + acctString()
        + ", acctStringDate=" + acctStringDate()
        + ", acctStringHour=" + acctStringHour()
        + ", acctStringTime=" + acctStringTime()
        + ", aMPCPUTime=" + aMPCPUTime()
        + ", aMPCPUTimeNorm=" + aMPCPUTimeNorm()
        + ", appID=" + appID()
        + ", cacheFlag=" + cacheFlag()
        + ", calendarName=" + calendarName()
        + ", callNestingLevel=" + callNestingLevel()
        + ", checkpointNum=" + checkpointNum()
        + ", clientAddr=" + clientAddr()
        + ", clientID=" + clientID()
        + ", collectTimeStamp=" + collectTimeStamp()
        + ", cPUDecayLevel=" + cPUDecayLevel()
        + ", dataCollectAlg=" + dataCollectAlg()
        + ", dBQLStatus=" + dBQLStatus()
        + ", defaultDatabase=" + defaultDatabase()
        + ", delayTime=" + delayTime()
        + ", disCPUTime=" + disCPUTime()
        + ", disCPUTimeNorm=" + disCPUTimeNorm()
        + ", errorCode=" + errorCode()
        + ", estMaxRowCount=" + estMaxRowCount()
        + ", estMaxStepTime=" + estMaxStepTime()
        + ", estProcTime=" + estProcTime()
        + ", estResultRows=" + estResultRows()
        + ", expandAcctString=" + expandAcctString()
        + ", firstRespTime=" + firstRespTime()
        + ", firstStepTime=" + firstStepTime()
        + ", flexThrottle=" + flexThrottle()
        + ", impactSpool=" + impactSpool()
        + ", internalRequestNum=" + internalRequestNum()
        + ", iODecayLevel=" + iODecayLevel()
        + ", iterationCount=" + iterationCount()
        + ", keepFlag=" + keepFlag()
        + ", lastRespTime=" + lastRespTime()
        + ", lockDelay=" + lockDelay()
        + ", lockLevel=" + lockLevel()
        + ", logicalHostID=" + logicalHostID()
        + ", logonDateTime=" + logonDateTime()
        + ", logonSource=" + logonSource()
        + ", lSN=" + lSN()
        + ", maxAMPCPUTime=" + maxAMPCPUTime()
        + ", maxAMPCPUTimeNorm=" + maxAMPCPUTimeNorm()
        + ", maxAmpIO=" + maxAmpIO()
        + ", maxCPUAmpNumber=" + maxCPUAmpNumber()
        + ", maxCPUAmpNumberNorm=" + maxCPUAmpNumberNorm()
        + ", maxIOAmpNumber=" + maxIOAmpNumber()
        + ", maxNumMapAMPs=" + maxNumMapAMPs()
        + ", maxOneMBRowSize=" + maxOneMBRowSize()
        + ", maxStepMemory=" + maxStepMemory()
        + ", maxStepsInPar=" + maxStepsInPar()
        + ", minAmpCPUTime=" + minAmpCPUTime()
        + ", minAmpCPUTimeNorm=" + minAmpCPUTimeNorm()
        + ", minAmpIO=" + minAmpIO()
        + ", minNumMapAMPs=" + minNumMapAMPs()
        + ", minRespHoldTime=" + minRespHoldTime()
        + ", numFragments=" + numFragments()
        + ", numOfActiveAMPs=" + numOfActiveAMPs()
        + ", numRequestCtx=" + numRequestCtx()
        + ", numResultOneMBRows=" + numResultOneMBRows()
        + ", numResultRows=" + numResultRows()
        + ", numSteps=" + numSteps()
        + ", numStepswPar=" + numStepswPar()
        + ", paramQuery=" + paramQuery()
        + ", parserCPUTime=" + parserCPUTime()
        + ", parserCPUTimeNorm=" + parserCPUTimeNorm()
        + ", parserExpReq=" + parserExpReq()
        + ", persistentSpool=" + persistentSpool()
        + ", procID=" + Arrays.toString(procID())
        + ", profileID=" + profileID()
        + ", profileName=" + profileName()
        + ", proxyRole=" + proxyRole()
        + ", proxyUser=" + proxyUser()
        + ", proxyUserID=" + proxyUserID()
        + ", queryBand=" + queryBand()
        + ", queryID=" + Arrays.toString(queryID())
        + ", queryRedriven=" + queryRedriven()
        + ", queryText=" + queryText()
        + ", reDriveKind=" + reDriveKind()
        + ", remoteQuery=" + remoteQuery()
        + ", reqIOKB=" + reqIOKB()
        + ", reqPhysIO=" + reqPhysIO()
        + ", reqPhysIOKB=" + reqPhysIOKB()
        + ", requestMode=" + requestMode()
        + ", requestNum=" + requestNum()
        + ", seqRespTime=" + seqRespTime()
        + ", sessionID=" + sessionID()
        + ", sessionTemporalQualifier=" + sessionTemporalQualifier()
        + ", spoolUsage=" + spoolUsage()
        + ", startTime=" + startTime()
        + ", statementGroup=" + statementGroup()
        + ", statements=" + statements()
        + ", statementType=" + statementType()
        + ", sysDefNumMapAMPs=" + sysDefNumMapAMPs()
        + ", tacticalCPUException=" + tacticalCPUException()
        + ", tacticalIOException=" + tacticalIOException()
        + ", tDWMEstMemUsage=" + tDWMEstMemUsage()
        + ", throttleBypassed=" + throttleBypassed()
        + ", totalFirstRespTime=" + totalFirstRespTime()
        + ", totalIOCount=" + totalIOCount()
        + ", totalServerByteCount=" + totalServerByteCount()
        + ", tTGranularity=" + tTGranularity()
        + ", txnMode=" + txnMode()
        + ", txnUniq=" + txnUniq()
        + ", unitySQL=" + unitySQL()
        + ", unityTime=" + unityTime()
        + ", usedIota=" + usedIota()
        + ", userID=" + Arrays.toString(userID())
        + ", userName=" + userName()
        + ", utilityByteCount=" + utilityByteCount()
        + ", utilityInfoAvailable=" + utilityInfoAvailable()
        + ", utilityRowCount=" + utilityRowCount()
        + ", vHLogicalIO=" + vHLogicalIO()
        + ", vHLogicalIOKB=" + vHLogicalIOKB()
        + ", vHPhysIO=" + vHPhysIO()
        + ", vHPhysIOKB=" + vHPhysIOKB()
        + ", warningOnly=" + warningOnly()
        + ", wDName=" + wDName()
        + "}" + lineSeparator();
  }

  public static QuerylogRow create(String abortFlag, String acctString, long acctStringDate,
      int acctStringHour, double acctStringTime, double aMPCPUTime, double aMPCPUTimeNorm,
      String appID, String cacheFlag, String calendarName, int callNestingLevel,
      double checkpointNum,
      String clientAddr, String clientID, long collectTimeStamp, int cPUDecayLevel,
      int dataCollectAlg, int dBQLStatus, String defaultDatabase, double delayTime,
      double disCPUTime,
      double disCPUTimeNorm, int errorCode, double estMaxRowCount, double estMaxStepTime,
      double estProcTime, double estResultRows, String expandAcctString, long firstRespTime,
      long firstStepTime, String flexThrottle, int impactSpool, int internalRequestNum,
      int iODecayLevel, int iterationCount, String keepFlag, long lastRespTime, double lockDelay,
      String lockLevel, int logicalHostID, long logonDateTime, String logonSource, double lSN,
      double maxAMPCPUTime, double maxAMPCPUTimeNorm, double maxAmpIO, int maxCPUAmpNumber,
      int maxCPUAmpNumberNorm, int maxIOAmpNumber, int maxNumMapAMPs, int maxOneMBRowSize,
      double maxStepMemory, int maxStepsInPar, double minAmpCPUTime, double minAmpCPUTimeNorm,
      double minAmpIO, int minNumMapAMPs, double minRespHoldTime, int numFragments,
      int numOfActiveAMPs, int numRequestCtx, double numResultOneMBRows, double numResultRows,
      int numSteps, int numStepswPar, String paramQuery, double parserCPUTime,
      double parserCPUTimeNorm, double parserExpReq, long persistentSpool, byte[] procID,
      String profileID, String profileName, String proxyRole, String proxyUser, String proxyUserID,
      String queryBand, byte[] queryID, String queryRedriven, String queryText, String reDriveKind,
      String remoteQuery, double reqIOKB, double reqPhysIO, double reqPhysIOKB, String requestMode,
      int requestNum, double seqRespTime, int sessionID, String sessionTemporalQualifier,
      long spoolUsage, long startTime, String statementGroup, int statements, String statementType,
      int sysDefNumMapAMPs, int tacticalCPUException, int tacticalIOException,
      double tDWMEstMemUsage,
      double throttleBypassed, double totalFirstRespTime, double totalIOCount,
      long totalServerByteCount, String tTGranularity, String txnMode, String txnUniq,
      String unitySQL, double unityTime, double usedIota, byte[] userID, String userName,
      int utilityByteCount, String utilityInfoAvailable, double utilityRowCount, double vHLogicalIO,
      double vHLogicalIOKB, double vHPhysIO, double vHPhysIOKB, String warningOnly, String wDName) {
    return new AutoValue_QuerylogRow(abortFlag, acctString, acctStringDate, acctStringHour,
        acctStringTime, aMPCPUTime, aMPCPUTimeNorm, appID, cacheFlag, calendarName,
        callNestingLevel, checkpointNum, clientAddr, clientID, collectTimeStamp, cPUDecayLevel,
        dataCollectAlg, dBQLStatus, defaultDatabase, delayTime, disCPUTime, disCPUTimeNorm,
        errorCode, estMaxRowCount, estMaxStepTime, estProcTime, estResultRows, expandAcctString,
        firstRespTime, firstStepTime, flexThrottle, impactSpool, internalRequestNum, iODecayLevel,
        iterationCount, keepFlag, lastRespTime, lockDelay, lockLevel, logicalHostID, logonDateTime,
        logonSource, lSN, maxAMPCPUTime, maxAMPCPUTimeNorm, maxAmpIO, maxCPUAmpNumber,
        maxCPUAmpNumberNorm, maxIOAmpNumber, maxNumMapAMPs, maxOneMBRowSize, maxStepMemory,
        maxStepsInPar, minAmpCPUTime, minAmpCPUTimeNorm, minAmpIO, minNumMapAMPs, minRespHoldTime,
        numFragments, numOfActiveAMPs, numRequestCtx, numResultOneMBRows, numResultRows, numSteps,
        numStepswPar, paramQuery, parserCPUTime, parserCPUTimeNorm, parserExpReq, persistentSpool,
        procID, profileID, profileName, proxyRole, proxyUser, proxyUserID, queryBand, queryID,
        queryRedriven, queryText, reDriveKind, remoteQuery, reqIOKB, reqPhysIO, reqPhysIOKB,
        requestMode, requestNum, seqRespTime, sessionID, sessionTemporalQualifier, spoolUsage,
        startTime, statementGroup, statements, statementType, sysDefNumMapAMPs,
        tacticalCPUException, tacticalIOException, tDWMEstMemUsage, throttleBypassed,
        totalFirstRespTime, totalIOCount, totalServerByteCount, tTGranularity, txnMode, txnUniq,
        unitySQL, unityTime, usedIota, userID, userName, utilityByteCount, utilityInfoAvailable,
        utilityRowCount, vHLogicalIO, vHLogicalIOKB, vHPhysIO, vHPhysIOKB, warningOnly, wDName);
  }


}
