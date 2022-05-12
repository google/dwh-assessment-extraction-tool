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
load("@rules_pkg//:mappings.bzl", "pkg_filegroup", "pkg_files", "strip_prefix")
load("@rules_pkg//:pkg.bzl", "pkg_zip")

package(default_visibility = ["//src:internal"])

filegroup(
    name = "third_party_dependencies",
    srcs = glob(
        ["**/*"],
        exclude = ["BUILD"],
    ),
)

pkg_files(
    name = "third_party_files",
    srcs = glob(
        ["**/*"],
        exclude = ["BUILD"],
    ),
    prefix = "third_party",
    strip_prefix = strip_prefix.from_pkg(),
)
