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
import static com.google.avro.AvroHelper.getIntNotNull;
import static com.google.avro.AvroHelper.getStringNotNull;
import static com.google.tdjdbc.JdbcHelper.getIntNotNull;
import static com.google.tdjdbc.JdbcHelper.getStringNotNull;
import static java.lang.String.format;

import com.google.base.TestBase;
import com.google.common.collect.LinkedHashMultiset;
import com.google.pojo.IndicesRow;
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

public class IndicesTest extends TestBase {

  private static Connection connection;
  private static final String SQL_PATH = SQL_REQUESTS_BASE_PATH + "indices.sql";
  private static final String AVRO_FILE_PATH = ET_OUTPUT_PATH + "indices.avro";

  @BeforeClass
  public static void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void indicesTest() throws SQLException, IOException {
    LinkedHashMultiset<IndicesRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<IndicesRow> avroList = LinkedHashMultiset.create();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(format(SqlHelper.getSql(SQL_PATH), DB_NAME, DB_NAME))) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        IndicesRow dbRow =
            IndicesRow.create(
                getStringNotNull(rs, "DataBaseName"),
                getStringNotNull(rs, "TableName"),
                getIntNotNull(rs, "IndexNumber"),
                getStringNotNull(rs, "IndexType"),
                getStringNotNull(rs, "IndexName"),
                getStringNotNull(rs, "ColumnName"),
                getIntNotNull(rs, "ColumnPosition"),
                getIntNotNull(rs, "AccessCount"),
                getStringNotNull(rs, "UniqueFlag"));
        dbList.add(dbRow);
      }
    }

    DataFileReader<GenericRecord> dataFileReader = extractAvroDataFromFile(AVRO_FILE_PATH);

    while (dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();

      IndicesRow avroRow =
          IndicesRow.create(
              getStringNotNull(record, "DataBaseName"),
              getStringNotNull(record, "TableName"),
              getIntNotNull(record, "IndexNumber"),
              getStringNotNull(record, "IndexType"),
              getStringNotNull(record, "IndexName"),
              getStringNotNull(record, "ColumnName"),
              getIntNotNull(record, "ColumnPosition"),
              getIntNotNull(record, "AccessCount"),
              getStringNotNull(record, "UniqueFlag"));
      avroList.add(avroRow);
    }

    LinkedHashMultiset<IndicesRow> dbListCopy = LinkedHashMultiset.create(dbList);
    avroList.forEach(dbList::remove);
    dbListCopy.forEach(avroList::remove);

    assertListsEqual(dbList, avroList, SQL_PATH, AVRO_FILE_PATH);
  }
}
