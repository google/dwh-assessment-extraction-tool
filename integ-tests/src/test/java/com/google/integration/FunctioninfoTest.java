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
import static com.google.avro.AvroHelper.getStringNotNull;
import static com.google.tdjdbc.JdbcHelper.getBytesNotNull;
import static com.google.tdjdbc.JdbcHelper.getIntNotNull;
import static com.google.tdjdbc.JdbcHelper.getStringNotNull;
import static java.lang.String.format;

import com.google.base.TestBase;
import com.google.common.collect.LinkedHashMultiset;
import com.google.pojo.FunctioninfoRow;
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

public class FunctioninfoTest extends TestBase {

  private static Connection connection;
  private static final String sqlPath = "src/main/java/com/google/sql/functioninfo.sql";
  private static final String avroFilePath = ET_OUTPUT_PATH + "functioninfo.avro";

  @BeforeClass
  public static void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void functioninfoTest() throws SQLException, IOException {
    LinkedHashMultiset<FunctioninfoRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<FunctioninfoRow> avroList = LinkedHashMultiset.create();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(format(SqlHelper.getSql(sqlPath), DB_NAME))) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        FunctioninfoRow dbRow = FunctioninfoRow.create(
            getStringNotNull(rs, "DatabaseName"),
            getStringNotNull(rs, "FunctionName"),
            getStringNotNull(rs, "SpecificName"),
            getBytesNotNull(rs, "FunctionId"),
            getIntNotNull(rs, "NumParameters"),
            getStringNotNull(rs, "ParameterDataTypes"),
            getStringNotNull(rs, "FunctionType"),
            getStringNotNull(rs, "ExternalName"),
            getStringNotNull(rs, "SrcFileLanguage"),
            getStringNotNull(rs, "NoSQLDataAccess"),
            getStringNotNull(rs, "ParameterStyle"),
            getStringNotNull(rs, "DeterministicOpt"),
            getStringNotNull(rs, "NullCall"),
            getStringNotNull(rs, "PrepareCount"),
            getStringNotNull(rs, "ExecProtectionMode"),
            getStringNotNull(rs, "ExtFileReference"),
            getIntNotNull(rs, "CharacterType"),
            getStringNotNull(rs, "Platform"),
            getIntNotNull(rs, "InterimFldSize"),
            getStringNotNull(rs, "RoutineKind"),
            getBytesNotNull(rs, "ParameterUDTIds"),
            getIntNotNull(rs, "MaxOutParameters"),
            getStringNotNull(rs, "GLOPSetDatabaseName"),
            getStringNotNull(rs, "GLOPSetMemberName"),
            getStringNotNull(rs, "RefQueryband"),
            getStringNotNull(rs, "ExecMapName"),
            getStringNotNull(rs, "ExecMapColocName"));
        dbList.add(dbRow);
      }
    }

    DataFileReader<GenericRecord> dataFileReader = extractAvroDataFromFile(avroFilePath);

    while (dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();

      FunctioninfoRow avroRow = FunctioninfoRow.create(
          getStringNotNull(record, "DatabaseName"),
          getStringNotNull(record, "FunctionName"),
          getStringNotNull(record, "SpecificName"),
          getBytesNotNull(record, "FunctionId"),
          getIntNotNull(record, "NumParameters"),
          getStringNotNull(record, "ParameterDataTypes"),
          getStringNotNull(record, "FunctionType"),
          getStringNotNull(record, "ExternalName"),
          getStringNotNull(record, "SrcFileLanguage"),
          getStringNotNull(record, "NoSQLDataAccess"),
          getStringNotNull(record, "ParameterStyle"),
          getStringNotNull(record, "DeterministicOpt"),
          getStringNotNull(record, "NullCall"),
          getStringNotNull(record, "PrepareCount"),
          getStringNotNull(record, "ExecProtectionMode"),
          getStringNotNull(record, "ExtFileReference"),
          getIntNotNull(record, "CharacterType"),
          getStringNotNull(record, "Platform"),
          getIntNotNull(record, "InterimFldSize"),
          getStringNotNull(record, "RoutineKind"),
          getBytesNotNull(record, "ParameterUDTIds"),
          getIntNotNull(record, "MaxOutParameters"),
          getStringNotNull(record, "GLOPSetDatabaseName"),
          getStringNotNull(record, "GLOPSetMemberName"),
          getStringNotNull(record, "RefQueryband"),
          getStringNotNull(record, "ExecMapName"),
          getStringNotNull(record, "ExecMapColocName"));
      avroList.add(avroRow);
    }

    LinkedHashMultiset<FunctioninfoRow> dbListCopy = LinkedHashMultiset.create(dbList);
    avroList.forEach(dbList::remove);
    dbListCopy.forEach(avroList::remove);

    assertListsEqual(dbList, avroList, sqlPath, avroFilePath);
  }
}
