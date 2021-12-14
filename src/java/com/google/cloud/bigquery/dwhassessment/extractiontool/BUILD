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
load("@rules_java//java:defs.bzl", "java_binary", "java_library", "java_test")

package(default_visibility = ["//src:internal"])

java_library(
    name = "extraction_tool",
    srcs = glob(["*.java"]),
    deps = [
        "//src/java/com/google/cloud/bigquery/dwhassessment/extractiontool/config",
        "//src/java/com/google/cloud/bigquery/dwhassessment/extractiontool/db",
        "@maven//:com_google_inject_guice",
        "@maven//:info_picocli_picocli",
        "@maven//:javax_inject_javax_inject_1",
    ],
)

java_binary(
    name = "ExtractionTool",
    main_class = "com.google.cloud.bigquery.dwhassessment.extractiontool.ExtractionTool",
    runtime_deps = [
        ":extraction_tool",
        "@maven//:org_slf4j_slf4j_jdk14",
    ],
)

java_binary(
    name = "ExtractionToolHsqldb",
    main_class = "com.google.cloud.bigquery.dwhassessment.extractiontool.ExtractionTool",
    runtime_deps = [
        ":extraction_tool",
        "@maven//:org_hsqldb_hsqldb",
        "@maven//:org_slf4j_slf4j_jdk14",
    ],
)
