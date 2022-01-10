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
import com.google.sql.SqlReader;
import com.google.tdjdbc.JdbcHelper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTests extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(IntegrationTests.class);
  private static Connection connection;

  @BeforeClass
  public static void beforeClass() throws ClassNotFoundException, SQLException {
    Class.forName("com.teradata.jdbc.TeraDriver");
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void usersTest() throws SQLException, IOException {
    final String sql = "src/main/java/com/google/sql/users.sql";
    final String avroFile = ET_OUTPUT_PATH + "users.avro";
    final String query = MessageFormat.format(SqlReader.getSql(sql), DB_NAME);

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

    DataFileReader<GenericRecord> dataFileReader = extractAvroDataFromFile(avroFile);

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

    StringBuilder dbListOutput = new StringBuilder().append('\n');
    StringBuilder avroListOutput = new StringBuilder().append('\n');

    dbList.forEach(e -> dbListOutput.append(e.toString()));
    avroList.forEach(e -> avroListOutput.append(e.toString()));

    if (dbList.isEmpty() && avroList.isEmpty()) {
      logger.info("DB view and Avro file are equal");
    } else if (!dbList.isEmpty() && !avroList.isEmpty()) {
      Assert.fail(
          format(
              "DB view and Avro file have mutually exclusive row(s) \n"
                  + "DB view '%s' has %d different row(s): %s"
                  + "\nAvro file %s has %d different row(s): %s",
              sql, dbList.size(), dbListOutput, avroFile, avroList.size(), avroListOutput));
    } else if (!dbList.isEmpty()) {
      Assert.fail(
          format("DB view '%s' has %d extra row(s): \n %s", sql, dbList.size(), dbListOutput));
    } else if (!avroList.isEmpty()) {
      Assert.fail(
          format(
              "Avro file %s has %d extra row(s): \n %s",
              avroFile, avroList.size(), avroListOutput));
    }
  }
}
