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
package com.google.cloud.bigquery.dwhassessment.extractiontool;

import com.google.cloud.bigquery.dwhassessment.extractiontool.config.BaseModule;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilter;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaFilters;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import java.util.Set;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/** Main class for the extraction tool. */
@Command(
    name = "dwet",
    mixinStandardHelpOptions = true,
    version = "dwet 0.1",
    description = "Tool to extract metadata from a data warehouse for assessment.")
public final class ExtractionTool implements Callable<Integer> {

  private ExtractionTool() {}

  @Override
  public Integer call() {
    return 0;
  }

  public static void main(String[] args) {
    Injector injector = Guice.createInjector(new BaseModule());

    CommandLine commandLine = new CommandLine(new ExtractionTool());
    Set<Callable<Integer>> subcommands = injector.getInstance(new Key<Set<Callable<Integer>>>() {});
    subcommands.forEach(commandLine::addSubcommand);
    commandLine.registerConverter(SchemaFilter.class, SchemaFilters::parse);

    System.exit(commandLine.execute(args));
  }
}
