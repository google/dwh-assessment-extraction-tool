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
package com.google.cloud.bigquery.dwhassessment.extractiontool.subcommand;

import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "about", description = "Provides information about this software")
public class AboutSubcommand implements Callable<Integer> {

  @Override
  public Integer call() throws IOException, SQLException {
    System.out.println("Data Warehouse Assessment Extraction Client");
    System.out.println();
    System.out.println(
        "This tool allows to extract meta information from a data warehouse that allows");
    System.out.println("to make an assessment for migration.");
    System.out.println();
    try (InputStreamReader reader =
        new InputStreamReader(this.getClass().getResource("/NOTICE.txt").openStream())) {
      for (String s : CharStreams.readLines(reader)) {
        System.out.println(s);
      }
    }
    return 0;
  }
}
