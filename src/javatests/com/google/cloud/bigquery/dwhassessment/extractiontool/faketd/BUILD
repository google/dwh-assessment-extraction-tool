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
    name = "faketd",
    srcs = glob(
        include = ["*.java"],
        exclude = ["*Test.java"],
    ),
    resources = glob([
        "*.sql",
    ]),
    deps = [
        "@maven//:org_hsqldb_sqltool",
    ],
)

java_library(
    name = "tests",
    srcs = glob(["*Test.java"]),
    deps = [
        ":faketd",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
    ],
)

java_test(
    name = "TeradataSimulatorTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.extractiontool.faketd.TeradataSimulatorTest",
    runtime_deps = [
        ":tests",
        "@maven//:org_hsqldb_hsqldb",
    ],
)
