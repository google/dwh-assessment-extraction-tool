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
package com.google.cloud.bigquery.dwhassessment.extractiontool.config;

import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.SchemaManagerImpl;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptManagerImpl;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptRunner;
import com.google.cloud.bigquery.dwhassessment.extractiontool.db.ScriptRunnerImpl;
import com.google.cloud.bigquery.dwhassessment.extractiontool.dbscripts.InternalScriptLoader;
import com.google.cloud.bigquery.dwhassessment.extractiontool.dbscripts.ScriptLoader;
import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManager;
import com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManagerFactory;
import com.google.cloud.bigquery.dwhassessment.extractiontool.executor.ExtractExecutor;
import com.google.cloud.bigquery.dwhassessment.extractiontool.executor.ExtractExecutorImpl;
import com.google.cloud.bigquery.dwhassessment.extractiontool.subcommand.AboutSubcommand;
import com.google.cloud.bigquery.dwhassessment.extractiontool.subcommand.ExtractSubcommand;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.inject.Provider;
import javax.inject.Singleton;

/** Base module for the extraction tool. */
public final class BaseModule extends AbstractModule {

  @Provides
  @Singleton
  ExtractExecutor extractExecutor(
      SchemaManager schemaManager,
      ScriptManager scriptManager,
      Function<Path, DataEntityManager> dataEntityManagerFactory) {
    return new ExtractExecutorImpl(schemaManager, scriptManager, dataEntityManagerFactory);
  }

  @Provides
  @Singleton
  ExtractSubcommand extractSubcommand(
      Provider<ExtractExecutor> extractExecutorProvider,
      ScriptManager scriptManager) {
    return new ExtractSubcommand(extractExecutorProvider::get, scriptManager);
  }

  @Provides
  @Singleton
  AboutSubcommand aboutSubcommand() {
    return new AboutSubcommand();
  }

  @Provides
  @Singleton
  SchemaManager schemaManager() {
    return new SchemaManagerImpl();
  }

  @Provides
  @Singleton
  ScriptManager scriptManager(
      ScriptRunner scriptRunner, ImmutableMap<String, Supplier<String>> scriptsMap) {
    return new ScriptManagerImpl(scriptRunner, scriptsMap);
  }

  @Provides
  @Singleton
  Function<Path, DataEntityManager> dataEntityManagerFactory() {
    return new DataEntityManagerFactory();
  }

  @Provides
  @Singleton
  ScriptRunner scriptRunner() {
    return new ScriptRunnerImpl();
  }

  @Provides
  @Singleton
  ScriptLoader scriptLoader() {
    return new InternalScriptLoader();
  }

  @Provides
  @Singleton
  ImmutableMap<String, Supplier<String>> getScriptsMap(ScriptLoader scriptLoader) {
    return scriptLoader.loadScripts();
  }

  @Override
  protected void configure() {
    Multibinder<Callable<Integer>> subcommandBinder =
        Multibinder.newSetBinder(binder(), new TypeLiteral<Callable<Integer>>() {});
    subcommandBinder.addBinding().to(new Key<AboutSubcommand>() {});
    subcommandBinder.addBinding().to(new Key<ExtractSubcommand>() {});
  }
}
