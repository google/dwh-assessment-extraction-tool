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
import static com.google.tdjdbc.JdbcUtil.getIntNotNull;
import static com.google.tdjdbc.JdbcUtil.getLongNotNull;
import static com.google.tdjdbc.JdbcUtil.getStringNotNull;
import static java.lang.String.format;

import com.google.base.TestBase;
import com.google.common.collect.LinkedHashMultiset;
import com.google.pojo.DiskspaceRow;
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

public class DiskspaceTest extends TestBase {

  private static Connection connection;
  private static final String SQL_PATH = SQL_REQUESTS_BASE_PATH + "diskspace.sql";
  private static final String AVRO_FILE_PATH = ET_OUTPUT_PATH + "diskspace.avro";

  @BeforeClass
  public static void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void diskspaceTest() throws SQLException, IOException {
    LinkedHashMultiset<DiskspaceRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<DiskspaceRow> avroList = LinkedHashMultiset.create();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(format(SqlUtil.getSql(SQL_PATH), DB_NAME, DB_NAME))) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        DiskspaceRow dbRow =
            DiskspaceRow.create(
                getIntNotNull(rs, "Vproc"),
                getStringNotNull(rs, "DatabaseName"),
                getStringNotNull(rs, "AccountName"),
                getLongNotNull(rs, "MaxPerm"),
                getLongNotNull(rs, "MaxSpool"),
                getLongNotNull(rs, "MaxTemp"),
                getLongNotNull(rs, "CurrentSpool"),
                getLongNotNull(rs, "CurrentPersistentSpool"),
                getLongNotNull(rs, "CurrentTemp"),
                getLongNotNull(rs, "PeakSpool"),
                getLongNotNull(rs, "PeakPersistentSpool"),
                getLongNotNull(rs, "PeakTemp"),
                getLongNotNull(rs, "MaxProfileSpool"),
                getLongNotNull(rs, "MaxProfileTemp"),
                getLongNotNull(rs, "AllocatedPerm"),
                getLongNotNull(rs, "AllocatedSpool"),
                getLongNotNull(rs, "AllocatedTemp"),
                getIntNotNull(rs, "PermSkew"),
                getIntNotNull(rs, "SpoolSkew"),
                getIntNotNull(rs, "TempSkew"));
        dbList.add(dbRow);
      }
    }

    DataFileReader<GenericRecord> dataFileReader = extractAvroDataFromFile(AVRO_FILE_PATH);

    while (dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();

      DiskspaceRow avroRow =
          DiskspaceRow.create(
              getIntNotNull(record, "Vproc"),
              getStringNotNull(record, "DatabaseName"),
              getStringNotNull(record, "AccountName"),
              getLongNotNull(record, "MaxPerm"),
              getLongNotNull(record, "MaxSpool"),
              getLongNotNull(record, "MaxTemp"),
              getLongNotNull(record, "CurrentSpool"),
              getLongNotNull(record, "CurrentPersistentSpool"),
              getLongNotNull(record, "CurrentTemp"),
              getLongNotNull(record, "PeakSpool"),
              getLongNotNull(record, "PeakPersistentSpool"),
              getLongNotNull(record, "PeakTemp"),
              getLongNotNull(record, "MaxProfileSpool"),
              getLongNotNull(record, "MaxProfileTemp"),
              getLongNotNull(record, "AllocatedPerm"),
              getLongNotNull(record, "AllocatedSpool"),
              getLongNotNull(record, "AllocatedTemp"),
              getIntNotNull(record, "PermSkew"),
              getIntNotNull(record, "SpoolSkew"),
              getIntNotNull(record, "TempSkew"));
      avroList.add(avroRow);
    }

    LinkedHashMultiset<DiskspaceRow> dbListCopy = LinkedHashMultiset.create(dbList);
    avroList.forEach(dbList::remove);
    dbListCopy.forEach(avroList::remove);

    assertListsEqual(dbList, avroList, SQL_PATH, AVRO_FILE_PATH);
  }
}
