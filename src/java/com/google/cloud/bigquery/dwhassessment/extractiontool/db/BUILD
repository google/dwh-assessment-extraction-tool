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
    name = "db",
    srcs = glob(["*.java"]),
    deps = [
        "//src:auto_value_plugin",
        "//src/java/com/google/cloud/bigquery/dwhassessment/extractiontool/dumper",
        "@maven//:com_github_jknack_handlebars",
        "@maven//:com_google_auto_value_auto_value",
        "@maven//:com_google_auto_value_auto_value_annotations",
        "@maven//:com_google_guava_guava_30_1_1_jre",
        "@maven//:com_google_re2j_re2j",
        "@maven//:org_apache_avro_avro",
    ],
)
