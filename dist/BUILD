load("@rules_pkg//:pkg.bzl", "pkg_zip")

pkg_zip(
    name = "dwh-assessment-extraction-tool",
    srcs = [
        "//:LICENSE",
        "//:README.md",
        "//src/java/com/google/cloud/bigquery/dwhassessment/extractiontool:ExtractionTool_deploy.jar",
        "//src/scripts:dwh-assessment-extraction-tool",
        "//src/scripts:run-td-extract",
    ],
    package_dir = "dwh-assessment-extraction-tool",
)
