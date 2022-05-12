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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "about", description = "Provides information about this software.")
public class AboutSubcommand implements Callable<Integer> {

  @CommandLine.Option(
      names = {"-h", "--help"},
      usageHelp = true)
  private boolean help;

  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public Integer call() throws IOException, SQLException {
    System.out.println("Data Warehouse Assessment Extraction Client");
    System.out.println();
    System.out.println(
        "This tool allows to extract meta information from a data warehouse that allows");
    System.out.println("to make an assessment for migration.");
    System.out.println();
    System.out.println("Data Warehouse Assessment Extraction Client");
    System.out.println("Copyright 2021 Google LLC");
    System.out.println();
    List<String> dependencyNames =
        Resources.readLines(
            AboutSubcommand.class.getResource("/third_party/dependency-list.txt"), UTF_8);
    for (String dependencyName : dependencyNames) {
      Dependency dependency =
          objectMapper.readValue(
              Resources.toString(
                  AboutSubcommand.class.getResource(
                      "/third_party/" + dependencyName + "/meta.json"),
                  UTF_8),
              Dependency.class);
      System.out.println(
          "------------------------------------------------------------------------");
      System.out.println(
          "This project includes software from " + dependency.projectName + " project.");
      System.out.println();
      System.out.println(dependency.projectUrl);
      if (!Objects.equals(dependency.projectUrl, dependency.repositoryUrl)) {
        System.out.println(dependency.repositoryUrl);
      }
      System.out.println();
      System.out.println(
          Resources.toString(
              AboutSubcommand.class.getResource("/third_party/" + dependencyName + "/LICENSE"),
              UTF_8));
    }
    return 0;
  }

  private static class Dependency {
    public String projectName;
    public String projectUrl;
    public String repositoryUrl;
  }
}
