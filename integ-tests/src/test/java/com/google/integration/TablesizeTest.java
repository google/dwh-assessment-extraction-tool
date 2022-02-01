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
import static com.google.avro.AvroHelper.getLongNotNull;
import static com.google.avro.AvroHelper.getStringNotNull;
import static com.google.tdjdbc.JdbcHelper.getLongNotNull;
import static com.google.tdjdbc.JdbcHelper.getStringNotNull;
import static java.lang.String.format;

import com.google.base.TestBase;
import com.google.common.collect.LinkedHashMultiset;
import com.google.pojo.TablesizeRow;
import com.google.sql.SqlHelper;
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

public class TablesizeTest extends TestBase {

  private static Connection connection;

  @BeforeClass
  public static void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void tablesizeTest() throws SQLException, IOException {
    final String sqlPath = "src/main/java/com/google/sql/tablesize.sql";
    final String avroFilePath = ET_OUTPUT_PATH + "tablesize.avro";

    LinkedHashMultiset<TablesizeRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<TablesizeRow> avroList = LinkedHashMultiset.create();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(format(SqlHelper.getSql(sqlPath), DB_NAME))) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        TablesizeRow dbRow = TablesizeRow.create(
            getStringNotNull(rs, "DataBaseName"),
            getStringNotNull(rs, "TableName"),
            getLongNotNull(rs, "CurrentPerm"),
            getLongNotNull(rs, "PeakPerm"));
        dbList.add(dbRow);
      }
    }

    DataFileReader<GenericRecord> dataFileReader = extractAvroDataFromFile(avroFilePath);

    while (dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();

      TablesizeRow avroRow = TablesizeRow.create(
          getStringNotNull(record, "DataBaseName"),
          getStringNotNull(record, "TableName"),
          getLongNotNull(record, "CurrentPerm"),
          getLongNotNull(record, "PeakPerm"));
      avroList.add(avroRow);
    }

    LinkedHashMultiset<TablesizeRow> dbListCopy = LinkedHashMultiset.create(dbList);
    avroList.forEach(dbList::remove);
    dbListCopy.forEach(avroList::remove);

    assertListsEqual(dbList, avroList, sqlPath, avroFilePath);
  }
}
