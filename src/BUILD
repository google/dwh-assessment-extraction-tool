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
load("@rules_java//java:defs.bzl", "java_library")

package_group(
    name = "internal",
    packages = [
        "//",
        "//dist/...",
        "//src/...",
    ],
)

package_group(
    name = "tests",
    packages = [
        "//src/javatests/...",
    ],
)

package(default_visibility = [":internal"])

java_plugin(
    name = "autovalue-plugin",
    generates_api = 1,
    processor_class = "com.google.auto.value.processor.AutoValueProcessor",
    deps = [
        "@maven//:com_google_auto_value_auto_value",
    ],
)

java_library(
    name = "auto_value_plugin",
    exported_plugins = [":autovalue-plugin"],
    neverlink = 1,
    exports = ["@maven//:com_google_auto_value_auto_value"],
)
