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

package(default_visibility = ["//src:tests"])

java_library(
    name = "dumpertesting",
    srcs = glob(
        include = ["*.java"],
        exclude = ["*Test.java"],
    ),
    deps = [
        "//src/java/com/google/cloud/bigquery/dwhassessment/extractiontool/dumper",
    ],
)

java_library(
    name = "tests",
    srcs = glob(["*Test.java"]),
    deps = [
        "//src/java/com/google/cloud/bigquery/dwhassessment/extractiontool/dumper",
        "@maven//:com_google_guava_guava_30_1_1_jre",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
    ],
)

java_test(
    name = "DataEntityManagerDirectoryImplTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManagerDirectoryImplTest",
    runtime_deps = [
        ":tests",
    ],
)

java_test(
    name = "DataEntityManagerZipImplTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManagerZipImplTest",
    runtime_deps = [
        ":tests",
    ],
)

java_test(
    name = "DataEntityManagerFactoryTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.extractiontool.dumper.DataEntityManagerFactoryTest",
    runtime_deps = [
        ":tests",
    ],
)
