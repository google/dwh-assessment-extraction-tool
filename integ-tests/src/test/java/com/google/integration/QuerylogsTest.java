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
package com.google.integration;

import static com.google.avro.AvroUtil.extractAvroDataFromFile;
import static com.google.avro.AvroUtil.getBytesNotNull;
import static com.google.avro.AvroUtil.getDoubleNotNull;
import static com.google.avro.AvroUtil.getIntNotNull;
import static com.google.avro.AvroUtil.getLongNotNull;
import static com.google.avro.AvroUtil.getStringNotNull;
import static com.google.sql.SqlUtil.getSql;
import static com.google.tdjdbc.JdbcUtil.getBytesNotNull;
import static com.google.tdjdbc.JdbcUtil.getDoubleNotNull;
import static com.google.tdjdbc.JdbcUtil.getIntNotNull;
import static com.google.tdjdbc.JdbcUtil.getLongNotNull;
import static com.google.tdjdbc.JdbcUtil.getStringNotNull;
import static com.google.tdjdbc.JdbcUtil.getTimestampNotNull;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.time.LocalDateTime.parse;
import static java.time.format.DateTimeFormatter.ofPattern;

import com.google.base.TestBase;
import com.google.common.collect.LinkedHashMultiset;
import com.google.pojo.QuerylogRow;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.BeforeClass;
import org.junit.Test;

public class QuerylogsTest extends TestBase {

  private static Connection connection;
  private static final String SQL_PATH = SQL_REQUESTS_BASE_PATH + "querylogs.sql";
  private static final String AVRO_FILE_PATH = ET_OUTPUT_PATH + "querylogs.avro";
  private static final String START_DATE = getenv("START_DATE");
  private static final String END_DATE = getenv("END_DATE");

  @BeforeClass
  public static void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void querylogsTest() throws SQLException, IOException {
    LinkedHashMultiset<QuerylogRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<QuerylogRow> avroList = LinkedHashMultiset.create();

    DateTimeFormatter iso8601Formatter = ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    DateTimeFormatter tdFormatter = ofPattern("yyyy-MM-dd HH:mm:ss");

    String startDate = parse(START_DATE, iso8601Formatter).format(tdFormatter);
    String endDate = parse(END_DATE, iso8601Formatter).format(tdFormatter);

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(
            format(getSql(SQL_PATH), DB_NAME, DB_NAME, startDate, endDate))) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        QuerylogRow dbRow =
            QuerylogRow.create(
                getStringNotNull(rs, "AbortFlag"),
                getStringNotNull(rs, "AcctString"),
                getLongNotNull(rs, "AcctStringDate"),
                getIntNotNull(rs, "AcctStringHour"),
                getDoubleNotNull(rs, "AcctStringTime"),
                getDoubleNotNull(rs, "AMPCPUTime"),
                getDoubleNotNull(rs, "AMPCPUTimeNorm"),
                getStringNotNull(rs, "AppID"),
                getStringNotNull(rs, "CacheFlag"),
                getStringNotNull(rs, "CalendarName"),
                getIntNotNull(rs, "CallNestingLevel"),
                getDoubleNotNull(rs, "CheckpointNum"),
                getStringNotNull(rs, "ClientAddr"),
                getStringNotNull(rs, "ClientID"),
                getTimestampNotNull(rs, "CollectTimeStamp"),
                getIntNotNull(rs, "CPUDecayLevel"),
                getIntNotNull(rs, "DataCollectAlg"),
                getIntNotNull(rs, "DBQLStatus"),
                getStringNotNull(rs, "DefaultDatabase"),
                getDoubleNotNull(rs, "DelayTime"),
                getDoubleNotNull(rs, "DisCPUTime"),
                getDoubleNotNull(rs, "DisCPUTimeNorm"),
                getIntNotNull(rs, "ErrorCode"),
                getDoubleNotNull(rs, "EstMaxRowCount"),
                getDoubleNotNull(rs, "EstMaxStepTime"),
                getDoubleNotNull(rs, "EstProcTime"),
                getDoubleNotNull(rs, "EstResultRows"),
                getStringNotNull(rs, "ExpandAcctString"),
                getTimestampNotNull(rs, "FirstRespTime"),
                getTimestampNotNull(rs, "FirstStepTime"),
                getStringNotNull(rs, "FlexThrottle"),
                getIntNotNull(rs, "ImpactSpool"),
                getIntNotNull(rs, "InternalRequestNum"),
                getIntNotNull(rs, "IODecayLevel"),
                getIntNotNull(rs, "IterationCount"),
                getStringNotNull(rs, "KeepFlag"),
                getLongNotNull(rs, "LastRespTime"),
                getDoubleNotNull(rs, "LockDelay"),
                getStringNotNull(rs, "LockLevel"),
                getIntNotNull(rs, "LogicalHostID"),
                getTimestampNotNull(rs, "LogonDateTime"),
                getStringNotNull(rs, "LogonSource"),
                getDoubleNotNull(rs, "LSN"),
                getDoubleNotNull(rs, "MaxAMPCPUTime"),
                getDoubleNotNull(rs, "MaxAMPCPUTimeNorm"),
                getDoubleNotNull(rs, "MaxAmpIO"),
                getIntNotNull(rs, "MaxCPUAmpNumber"),
                getIntNotNull(rs, "MaxCPUAmpNumberNorm"),
                getIntNotNull(rs, "MaxIOAmpNumber"),
                getIntNotNull(rs, "MaxNumMapAMPs"),
                getIntNotNull(rs, "MaxOneMBRowSize"),
                getDoubleNotNull(rs, "MaxStepMemory"),
                getIntNotNull(rs, "MaxStepsInPar"),
                getDoubleNotNull(rs, "MinAmpCPUTime"),
                getDoubleNotNull(rs, "MinAmpCPUTimeNorm"),
                getDoubleNotNull(rs, "MinAmpIO"),
                getIntNotNull(rs, "MinNumMapAMPs"),
                getDoubleNotNull(rs, "MinRespHoldTime"),
                getIntNotNull(rs, "NumFragments"),
                getIntNotNull(rs, "NumOfActiveAMPs"),
                getIntNotNull(rs, "NumRequestCtx"),
                getDoubleNotNull(rs, "NumResultOneMBRows"),
                getDoubleNotNull(rs, "NumResultRows"),
                getIntNotNull(rs, "NumSteps"),
                getIntNotNull(rs, "NumStepswPar"),
                getStringNotNull(rs, "ParamQuery"),
                getDoubleNotNull(rs, "ParserCPUTime"),
                getDoubleNotNull(rs, "ParserCPUTimeNorm"),
                getDoubleNotNull(rs, "ParserExpReq"),
                getLongNotNull(rs, "PersistentSpool"),
                getBytesNotNull(rs, "ProcID"),
                getStringNotNull(rs, "ProfileID"),
                getStringNotNull(rs, "ProfileName"),
                getStringNotNull(rs, "ProxyRole"),
                getStringNotNull(rs, "ProxyUser"),
                getStringNotNull(rs, "ProxyUserID"),
                getStringNotNull(rs, "QueryBand"),
                getBytesNotNull(rs, "QueryID"),
                getStringNotNull(rs, "QueryRedriven"),
                getStringNotNull(rs, "QueryText"),
                getStringNotNull(rs, "ReDriveKind"),
                getStringNotNull(rs, "RemoteQuery"),
                getDoubleNotNull(rs, "ReqIOKB"),
                getDoubleNotNull(rs, "ReqPhysIO"),
                getDoubleNotNull(rs, "ReqPhysIOKB"),
                getStringNotNull(rs, "RequestMode"),
                getIntNotNull(rs, "RequestNum"),
                getDoubleNotNull(rs, "SeqRespTime"),
                getIntNotNull(rs, "SessionID"),
                getStringNotNull(rs, "SessionTemporalQualifier"),
                getLongNotNull(rs, "SpoolUsage"),
                getTimestampNotNull(rs, "StartTime"),
                getStringNotNull(rs, "StatementGroup"),
                getIntNotNull(rs, "Statements"),
                getStringNotNull(rs, "StatementType"),
                getIntNotNull(rs, "SysDefNumMapAMPs"),
                getIntNotNull(rs, "TacticalCPUException"),
                getIntNotNull(rs, "TacticalIOException"),
                getDoubleNotNull(rs, "TDWMEstMemUsage"),
                getDoubleNotNull(rs, "ThrottleBypassed"),
                getDoubleNotNull(rs, "TotalFirstRespTime"),
                getDoubleNotNull(rs, "TotalIOCount"),
                getLongNotNull(rs, "TotalServerByteCount"),
                getStringNotNull(rs, "TTGranularity"),
                getStringNotNull(rs, "TxnMode"),
                getStringNotNull(rs, "TxnUniq"),
                getStringNotNull(rs, "UnitySQL"),
                getDoubleNotNull(rs, "UnityTime"),
                getDoubleNotNull(rs, "UsedIota"),
                getBytesNotNull(rs, "UserID"),
                getStringNotNull(rs, "UserName"),
                getIntNotNull(rs, "UtilityByteCount"),
                getStringNotNull(rs, "UtilityInfoAvailable"),
                getDoubleNotNull(rs, "UtilityRowCount"),
                getDoubleNotNull(rs, "VHLogicalIO"),
                getDoubleNotNull(rs, "VHLogicalIOKB"),
                getDoubleNotNull(rs, "VHPhysIO"),
                getDoubleNotNull(rs, "VHPhysIOKB"),
                getStringNotNull(rs, "WarningOnly"),
                getStringNotNull(rs, "WDName"));
        dbList.add(dbRow);
      }
    }

    DataFileReader<GenericRecord> dataFileReader = extractAvroDataFromFile(AVRO_FILE_PATH);

    while (dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();

      QuerylogRow avroRow =
          QuerylogRow.create(
              getStringNotNull(record, "AbortFlag"),
              getStringNotNull(record, "AcctString"),
              getLongNotNull(record, "AcctStringDate"),
              getIntNotNull(record, "AcctStringHour"),
              getDoubleNotNull(record, "AcctStringTime"),
              getDoubleNotNull(record, "AMPCPUTime"),
              getDoubleNotNull(record, "AMPCPUTimeNorm"),
              getStringNotNull(record, "AppID"),
              getStringNotNull(record, "CacheFlag"),
              getStringNotNull(record, "CalendarName"),
              getIntNotNull(record, "CallNestingLevel"),
              getDoubleNotNull(record, "CheckpointNum"),
              getStringNotNull(record, "ClientAddr"),
              getStringNotNull(record, "ClientID"),
              getLongNotNull(record, "CollectTimeStamp"),
              getIntNotNull(record, "CPUDecayLevel"),
              getIntNotNull(record, "DataCollectAlg"),
              getIntNotNull(record, "DBQLStatus"),
              getStringNotNull(record, "DefaultDatabase"),
              getDoubleNotNull(record, "DelayTime"),
              getDoubleNotNull(record, "DisCPUTime"),
              getDoubleNotNull(record, "DisCPUTimeNorm"),
              getIntNotNull(record, "ErrorCode"),
              getDoubleNotNull(record, "EstMaxRowCount"),
              getDoubleNotNull(record, "EstMaxStepTime"),
              getDoubleNotNull(record, "EstProcTime"),
              getDoubleNotNull(record, "EstResultRows"),
              getStringNotNull(record, "ExpandAcctString"),
              getLongNotNull(record, "FirstRespTime"),
              getLongNotNull(record, "FirstStepTime"),
              getStringNotNull(record, "FlexThrottle"),
              getIntNotNull(record, "ImpactSpool"),
              getIntNotNull(record, "InternalRequestNum"),
              getIntNotNull(record, "IODecayLevel"),
              getIntNotNull(record, "IterationCount"),
              getStringNotNull(record, "KeepFlag"),
              getLongNotNull(record, "LastRespTime"),
              getDoubleNotNull(record, "LockDelay"),
              getStringNotNull(record, "LockLevel"),
              getIntNotNull(record, "LogicalHostID"),
              getLongNotNull(record, "LogonDateTime"),
              getStringNotNull(record, "LogonSource"),
              getDoubleNotNull(record, "LSN"),
              getDoubleNotNull(record, "MaxAMPCPUTime"),
              getDoubleNotNull(record, "MaxAMPCPUTimeNorm"),
              getDoubleNotNull(record, "MaxAmpIO"),
              getIntNotNull(record, "MaxCPUAmpNumber"),
              getIntNotNull(record, "MaxCPUAmpNumberNorm"),
              getIntNotNull(record, "MaxIOAmpNumber"),
              getIntNotNull(record, "MaxNumMapAMPs"),
              getIntNotNull(record, "MaxOneMBRowSize"),
              getDoubleNotNull(record, "MaxStepMemory"),
              getIntNotNull(record, "MaxStepsInPar"),
              getDoubleNotNull(record, "MinAmpCPUTime"),
              getDoubleNotNull(record, "MinAmpCPUTimeNorm"),
              getDoubleNotNull(record, "MinAmpIO"),
              getIntNotNull(record, "MinNumMapAMPs"),
              getDoubleNotNull(record, "MinRespHoldTime"),
              getIntNotNull(record, "NumFragments"),
              getIntNotNull(record, "NumOfActiveAMPs"),
              getIntNotNull(record, "NumRequestCtx"),
              getDoubleNotNull(record, "NumResultOneMBRows"),
              getDoubleNotNull(record, "NumResultRows"),
              getIntNotNull(record, "NumSteps"),
              getIntNotNull(record, "NumStepswPar"),
              getStringNotNull(record, "ParamQuery"),
              getDoubleNotNull(record, "ParserCPUTime"),
              getDoubleNotNull(record, "ParserCPUTimeNorm"),
              getDoubleNotNull(record, "ParserExpReq"),
              getLongNotNull(record, "PersistentSpool"),
              getBytesNotNull(record, "ProcID"),
              getStringNotNull(record, "ProfileID"),
              getStringNotNull(record, "ProfileName"),
              getStringNotNull(record, "ProxyRole"),
              getStringNotNull(record, "ProxyUser"),
              getStringNotNull(record, "ProxyUserID"),
              getStringNotNull(record, "QueryBand"),
              getBytesNotNull(record, "QueryID"),
              getStringNotNull(record, "QueryRedriven"),
              getStringNotNull(record, "QueryText"),
              getStringNotNull(record, "ReDriveKind"),
              getStringNotNull(record, "RemoteQuery"),
              getDoubleNotNull(record, "ReqIOKB"),
              getDoubleNotNull(record, "ReqPhysIO"),
              getDoubleNotNull(record, "ReqPhysIOKB"),
              getStringNotNull(record, "RequestMode"),
              getIntNotNull(record, "RequestNum"),
              getDoubleNotNull(record, "SeqRespTime"),
              getIntNotNull(record, "SessionID"),
              getStringNotNull(record, "SessionTemporalQualifier"),
              getLongNotNull(record, "SpoolUsage"),
              getLongNotNull(record, "StartTime"),
              getStringNotNull(record, "StatementGroup"),
              getIntNotNull(record, "Statements"),
              getStringNotNull(record, "StatementType"),
              getIntNotNull(record, "SysDefNumMapAMPs"),
              getIntNotNull(record, "TacticalCPUException"),
              getIntNotNull(record, "TacticalIOException"),
              getDoubleNotNull(record, "TDWMEstMemUsage"),
              getDoubleNotNull(record, "ThrottleBypassed"),
              getDoubleNotNull(record, "TotalFirstRespTime"),
              getDoubleNotNull(record, "TotalIOCount"),
              getLongNotNull(record, "TotalServerByteCount"),
              getStringNotNull(record, "TTGranularity"),
              getStringNotNull(record, "TxnMode"),
              getStringNotNull(record, "TxnUniq"),
              getStringNotNull(record, "UnitySQL"),
              getDoubleNotNull(record, "UnityTime"),
              getDoubleNotNull(record, "UsedIota"),
              getBytesNotNull(record, "UserID"),
              getStringNotNull(record, "UserName"),
              getIntNotNull(record, "UtilityByteCount"),
              getStringNotNull(record, "UtilityInfoAvailable"),
              getDoubleNotNull(record, "UtilityRowCount"),
              getDoubleNotNull(record, "VHLogicalIO"),
              getDoubleNotNull(record, "VHLogicalIOKB"),
              getDoubleNotNull(record, "VHPhysIO"),
              getDoubleNotNull(record, "VHPhysIOKB"),
              getStringNotNull(record, "WarningOnly"),
              getStringNotNull(record, "WDName"));
      avroList.add(avroRow);
    }

    LinkedHashMultiset<QuerylogRow> dbListCopy = LinkedHashMultiset.create(dbList);
    avroList.forEach(dbList::remove);
    dbListCopy.forEach(avroList::remove);

    assertListsEqual(dbList, avroList, SQL_PATH, AVRO_FILE_PATH);
  }
}
