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
    deps = [
        "//src/java/com/google/cloud/bigquery/dwhassessment/extractiontool/db",
        "//src/java/com/google/cloud/bigquery/dwhassessment/extractiontool/dumper",
        "//src/java/com/google/cloud/bigquery/dwhassessment/extractiontool/executor",
        "@maven//:com_google_guava_guava_30_1_1_jre",
        "@maven//:com_google_re2j_re2j",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_hsqldb_hsqldb",
        "@maven//:org_mockito_mockito_core",
    ],
)

java_test(
    name = "ExtractExecutorImplTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.extractiontool.executor.ExtractExecutorImplTest",
    runtime_deps = [
        ":tests",
    ],
)
