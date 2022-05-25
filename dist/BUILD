load("@rules_pkg//:mappings.bzl", "pkg_attributes", "pkg_filegroup", "pkg_files", "strip_prefix")
load("@rules_pkg//:pkg.bzl", "pkg_zip")

pkg_files(
    name = "tool_files",
    srcs = [
        "//:LICENSE",
        "//:README.md",
        "//src/java/com/google/cloud/bigquery/dwhassessment/extractiontool:ExtractionTool_deploy.jar",
        "//src/scripts:dwh-assessment-extraction-tool",
        "//src/scripts:run-td-extract",
    ],
    attributes = pkg_attributes(
        mode = "0555",
    ),
    prefix = "",
)

pkg_zip(
    name = "dwh-assessment-extraction-tool",
    srcs = [
        ":tool_files",
        "//third_party:third_party_files",
    ],
    package_dir = "dwh-assessment-extraction-tool",
)
