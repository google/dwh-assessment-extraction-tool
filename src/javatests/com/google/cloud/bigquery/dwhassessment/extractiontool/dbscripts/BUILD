# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
load("@rules_java//java:defs.bzl", "java_library", "java_test")

java_library(
    name = "tests",
    srcs = glob(["*.java"]),
    resources = glob(["*.sql"]),
    deps = [
        "//src/java/com/google/cloud/bigquery/dwhassessment/extractiontool/db",
        "//src/java/com/google/cloud/bigquery/dwhassessment/extractiontool/dbscripts",
        "//src/javatests/com/google/cloud/bigquery/dwhassessment/extractiontool/dumper:dumpertesting",
        "//src/javatests/com/google/cloud/bigquery/dwhassessment/extractiontool/faketd",
        "@maven//:com_google_guava_guava_30_1_1_jre",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_avro_avro",
        "@maven//:org_mockito_mockito_core",
    ],
)

java_test(
    name = "InternalScriptLoaderTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.extractiontool.dbscripts.InternalScriptLoaderTest",
    runtime_deps = [
        ":tests",
        "@maven//:org_hsqldb_hsqldb",
    ],
)
