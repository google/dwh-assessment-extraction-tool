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

import static com.google.avro.AvroHelper.extractAvroDataFromFile;
import static java.lang.String.format;

import com.google.avro.AvroHelper;
import com.google.base.TestBase;
import com.google.pojo.UserTableRow;
import com.google.sql.SqlHelper;
import com.google.tdjdbc.JdbcHelper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.BeforeClass;
import org.junit.Test;

public class IntegrationTests2 extends TestBase {

  private static Connection connection;

  @BeforeClass
  public static void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void usersTest2() throws SQLException, IOException {
    final String sqlPath = "src/main/java/com/google/sql/users.sql";
    final String avroFilePath = ET_OUTPUT_PATH + "users.avro";

    List<UserTableRow> dbList = new ArrayList<>();
    List<UserTableRow> avroList = new ArrayList<>();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(format(SqlHelper.getSql(sqlPath), DB_NAME))) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        UserTableRow dbRow = new UserTableRow();
        dbRow.userName.name = rs.getString("UserName");
        dbRow.creatorName.name = rs.getString("CreatorName");
        dbRow.createTimeStamp.timestamp =
            getTimestampByDate(JdbcHelper.getStringNotNull(rs, "CreateTimeStamp"));
        dbRow.lastAccessTimeStamp.timestamp =
            getTimestampByDate(JdbcHelper.getStringNotNull(rs, "LastAccessTimeStamp"));
        dbList.add(dbRow);
      }
    }

    DataFileReader<GenericRecord> dataFileReader = extractAvroDataFromFile(avroFilePath);

    while (dataFileReader.hasNext()) {
      GenericRecord record = null;
      record = dataFileReader.next(record);

      UserTableRow avroRow = new UserTableRow();
      avroRow.userName.name = record.get("UserName").toString();
      avroRow.creatorName.name = record.get("CreatorName").toString();
      avroRow.createTimeStamp.timestamp = AvroHelper.getLongNotNull(record, "CreateTimeStamp");
      avroRow.lastAccessTimeStamp.timestamp =
          AvroHelper.getLongNotNull(record, "LastAccessTimeStamp");
      avroList.add(avroRow);
    }

    List<UserTableRow> dbListCopy = new ArrayList<>(dbList);
    avroList.forEach(dbList::remove);
    dbListCopy.forEach(avroList::remove);

    avroList.remove(0);
    assertListsEqual(dbList, avroList, sqlPath, avroFilePath);
  }
}
