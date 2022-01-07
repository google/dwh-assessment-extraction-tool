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

import com.google.avro.AvroHelper;
import com.google.base.TestBase;
import com.google.pojo.UserTableRow;
import com.google.sql.SqlReader;
import com.google.tdjdbc.JdbcHelper;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import static com.google.avro.AvroHelper.extractAvroDataFromFile;

public class IntegrationTests extends TestBase {

  private static Connection connection;

  @BeforeClass
  public static void beforeClass() throws ClassNotFoundException, SQLException {
    Class.forName("com.teradata.jdbc.TeraDriver");
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void usersTest() throws SQLException, IOException {
    String query =
        MessageFormat.format(SqlReader.getSql("src/main/java/com/google/sql/users.sql"), DB_NAME);
    List<UserTableRow> dbList = new ArrayList<>();
    List<UserTableRow> avroList = new ArrayList<>();

    try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
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

    DataFileReader<GenericRecord> dataFileReader =
        extractAvroDataFromFile(ET_OUTPUT_PATH + "users.avro");

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
  }
}
