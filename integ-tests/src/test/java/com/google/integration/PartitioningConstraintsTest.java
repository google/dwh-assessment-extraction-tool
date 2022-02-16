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
import static com.google.avro.AvroUtil.getIntNotNull;
import static com.google.avro.AvroUtil.getLongNotNull;
import static com.google.avro.AvroUtil.getStringNotNull;
import static com.google.base.TestConstants.DB_NAME;
import static com.google.base.TestConstants.ET_OUTPUT_PATH;
import static com.google.base.TestConstants.PASSWORD_DB;
import static com.google.base.TestConstants.SQL_REQUESTS_BASE_PATH;
import static com.google.base.TestConstants.URL_DB;
import static com.google.base.TestConstants.USERNAME_DB;
import static com.google.tdjdbc.JdbcUtil.getIntNotNull;
import static com.google.tdjdbc.JdbcUtil.getLongNotNull;
import static com.google.tdjdbc.JdbcUtil.getStringNotNull;
import static com.google.tdjdbc.JdbcUtil.getTimestampNotNull;
import static java.lang.String.format;

import com.google.base.TestBase;
import com.google.common.collect.LinkedHashMultiset;
import com.google.pojo.PartitioningConstraintsRow;
import com.google.sql.SqlUtil;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.BeforeClass;
import org.junit.Test;

public class PartitioningConstraintsTest extends TestBase {

  private static Connection connection;
  private static final String SQL_PATH = SQL_REQUESTS_BASE_PATH + "partitioning_constraints.sql";
  private static final String AVRO_FILE_PATH = ET_OUTPUT_PATH + "partitioning_constraints.avro";

  @BeforeClass
  public static void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void partitioningConstraintsTest() throws SQLException, IOException {
    LinkedHashMultiset<PartitioningConstraintsRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<PartitioningConstraintsRow> avroList = LinkedHashMultiset.create();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(format(SqlUtil.getSql(SQL_PATH), DB_NAME))) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        PartitioningConstraintsRow dbRow =
            PartitioningConstraintsRow.create(
                getStringNotNull(rs, "DatabaseName"),
                getStringNotNull(rs, "TableName"),
                getStringNotNull(rs, "IndexName"),
                getIntNotNull(rs, "IndexNumber"),
                getStringNotNull(rs, "ConstraintType"),
                getStringNotNull(rs, "ConstraintText"),
                getStringNotNull(rs, "ConstraintCollation"),
                getStringNotNull(rs, "CollationName"),
                getStringNotNull(rs, "CreatorName"),
                getTimestampNotNull(rs, "CreateTimeStamp"),
                getStringNotNull(rs, "CharSetID"),
                getStringNotNull(rs, "SessionMode"),
                getTimestampNotNull(rs, "ResolvedCurrent_Date"),
                getLongNotNull(rs, "ResolvedCurrent_TimeStamp"),
                getLongNotNull(rs, "DefinedCombinedPartitions"),
                getLongNotNull(rs, "MaxCombinedPartitions"),
                getIntNotNull(rs, "PartitioningLevels"),
                getIntNotNull(rs, "ColumnPartitioningLevel"));
        dbList.add(dbRow);
      }
    }

    DataFileReader<GenericRecord> dataFileReader = extractAvroDataFromFile(AVRO_FILE_PATH);

    while (dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();

      PartitioningConstraintsRow avroRow =
          PartitioningConstraintsRow.create(
              getStringNotNull(record, "DatabaseName"),
              getStringNotNull(record, "TableName"),
              getStringNotNull(record, "IndexName"),
              getIntNotNull(record, "IndexNumber"),
              getStringNotNull(record, "ConstraintType"),
              getStringNotNull(record, "ConstraintText"),
              getStringNotNull(record, "ConstraintCollation"),
              getStringNotNull(record, "CollationName"),
              getStringNotNull(record, "CreatorName"),
              getLongNotNull(record, "CreateTimeStamp"),
              getStringNotNull(record, "CharSetID"),
              getStringNotNull(record, "SessionMode"),
              getLongNotNull(record, "ResolvedCurrent_Date"),
              getLongNotNull(record, "ResolvedCurrent_TimeStamp"),
              getLongNotNull(record, "DefinedCombinedPartitions"),
              getLongNotNull(record, "MaxCombinedPartitions"),
              getIntNotNull(record, "PartitioningLevels"),
              getIntNotNull(record, "ColumnPartitioningLevel"));
      avroList.add(avroRow);
    }

    LinkedHashMultiset<PartitioningConstraintsRow> dbListCopy = LinkedHashMultiset.create(dbList);
    avroList.forEach(dbList::remove);
    dbListCopy.forEach(avroList::remove);

    assertListsEqual(dbList, avroList, SQL_PATH, AVRO_FILE_PATH);
  }
}
