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
import static com.google.avro.AvroHelper.getBytesNotNull;
import static com.google.avro.AvroHelper.getIntNotNull;
import static com.google.avro.AvroHelper.getLongNotNull;
import static com.google.avro.AvroHelper.getStringNotNull;
import static com.google.sql.SqlHelper.getSql;
import static com.google.tdjdbc.JdbcHelper.getBytesNotNull;
import static com.google.tdjdbc.JdbcHelper.getIntNotNull;
import static com.google.tdjdbc.JdbcHelper.getStringNotNull;
import static com.google.tdjdbc.JdbcHelper.getTimestampNotNull;
import static java.lang.String.format;

import com.google.base.TestBase;
import com.google.common.collect.LinkedHashMultiset;
import com.google.pojo.QueryReferencesRow;
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

public final class QueryReferencesTest extends TestBase {

  private static Connection connection;
  private static final String SQL_PATH = SQL_REQUESTS_BASE_PATH + "query_references.sql";
  private static final String AVRO_FILE_PATH = ET_OUTPUT_PATH + "query_references.avro";

  @BeforeClass
  public static void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void queryReferencesTest() throws SQLException, IOException {
    LinkedHashMultiset<QueryReferencesRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<QueryReferencesRow> avroList = LinkedHashMultiset.create();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(format(getSql(SQL_PATH), DB_NAME))) {
      ResultSet rs = preparedStatement.executeQuery();
      while (rs.next()) {
        QueryReferencesRow dbRow = QueryReferencesRow.create(
            getBytesNotNull(rs, "ProcID"),
            getTimestampNotNull(rs, "CollectTimeStamp"),
            getBytesNotNull(rs, "QueryID"),
            getStringNotNull(rs, "ObjectDatabaseName"),
            getStringNotNull(rs, "ObjectTableName"),
            getStringNotNull(rs, "ObjectColumnName"),
            getBytesNotNull(rs, "ObjectID"),
            getIntNotNull(rs, "ObjectNum"),
            getStringNotNull(rs, "ObjectType"),
            getIntNotNull(rs, "FreqofUse"),
            getIntNotNull(rs, "TypeofUse"));
        dbList.add(dbRow);
      }
    }

    DataFileReader<GenericRecord> dataFileReader = extractAvroDataFromFile(AVRO_FILE_PATH);

    while (dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();

      QueryReferencesRow avroRow = QueryReferencesRow.create(
          getBytesNotNull(record, "ProcID"),
          getLongNotNull(record, "CollectTimeStamp"),
          getBytesNotNull(record, "QueryID"),
          getStringNotNull(record, "ObjectDatabaseName"),
          getStringNotNull(record, "ObjectTableName"),
          getStringNotNull(record, "ObjectColumnName"),
          getBytesNotNull(record, "ObjectID"),
          getIntNotNull(record, "ObjectNum"),
          getStringNotNull(record, "ObjectType"),
          getIntNotNull(record, "FreqofUse"),
          getIntNotNull(record, "TypeofUse"));
      avroList.add(avroRow);
    }

    LinkedHashMultiset<QueryReferencesRow> dbListCopy = LinkedHashMultiset.create(dbList);
    avroList.forEach(dbList::remove);
    dbListCopy.forEach(avroList::remove);

    assertListsEqual(dbList, avroList, SQL_PATH, AVRO_FILE_PATH);
  }
}
