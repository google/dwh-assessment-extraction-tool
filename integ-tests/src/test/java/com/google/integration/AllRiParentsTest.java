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
import static com.google.tdjdbc.JdbcUtil.getStringNotNull;
import static com.google.tdjdbc.JdbcUtil.getTimestampNotNull;
import static java.lang.String.format;

import com.google.base.TestBase;
import com.google.common.collect.LinkedHashMultiset;
import com.google.pojo.AllRiChildrenRow;
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

public class AllRiParentsTest extends TestBase {

  private static Connection connection;
  private static final String SQL_PATH = SQL_REQUESTS_BASE_PATH + "all_ri_parents.sql";
  private static final String AVRO_FILE_PATH = ET_OUTPUT_PATH + "all_ri_parents.avro";

  @BeforeClass
  public static void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void allRiParentsTest() throws SQLException, IOException {
    LinkedHashMultiset<AllRiChildrenRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<AllRiChildrenRow> avroList = LinkedHashMultiset.create();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(format(SqlUtil.getSql(SQL_PATH), DB_NAME))) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        AllRiChildrenRow dbRow =
            AllRiChildrenRow.create(
                getIntNotNull(rs, "IndexID"),
                getStringNotNull(rs, "IndexName"),
                getStringNotNull(rs, "ChildDB"),
                getStringNotNull(rs, "ChildTable"),
                getStringNotNull(rs, "ChildKeyColumn"),
                getStringNotNull(rs, "ParentDB"),
                getStringNotNull(rs, "ParentTable"),
                getStringNotNull(rs, "ParentKeyColumn"),
                getStringNotNull(rs, "InconsistencyFlag"),
                getStringNotNull(rs, "CreatorName"),
                getTimestampNotNull(rs, "CreateTimeStamp"));
        dbList.add(dbRow);
      }
    }

    DataFileReader<GenericRecord> dataFileReader = extractAvroDataFromFile(AVRO_FILE_PATH);

    while (dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();

      AllRiChildrenRow avroRow =
          AllRiChildrenRow.create(
              getIntNotNull(record, "IndexID"),
              getStringNotNull(record, "IndexName"),
              getStringNotNull(record, "ChildDB"),
              getStringNotNull(record, "ChildTable"),
              getStringNotNull(record, "ChildKeyColumn"),
              getStringNotNull(record, "ParentDB"),
              getStringNotNull(record, "ParentTable"),
              getStringNotNull(record, "ParentKeyColumn"),
              getStringNotNull(record, "InconsistencyFlag"),
              getStringNotNull(record, "CreatorName"),
              getLongNotNull(record, "CreateTimeStamp"));
      avroList.add(avroRow);
    }

    LinkedHashMultiset<AllRiChildrenRow> dbListCopy = LinkedHashMultiset.create(dbList);
    avroList.forEach(dbList::remove);
    dbListCopy.forEach(avroList::remove);

    assertListsEqual(dbList, avroList, SQL_PATH, AVRO_FILE_PATH);
  }
}
