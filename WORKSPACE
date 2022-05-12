load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

RULES_JVM_EXTERNAL_TAG = "4.0"

RULES_JVM_EXTERNAL_SHA = "31701ad93dbfe544d597dbe62c9a1fdd76d81d8a9150c2bf1ecf928ecdf97169"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = [
        "com.github.jknack:handlebars:4.3.0",
        "com.google.auto.value:auto-value:1.8.1",
        "com.google.auto.value:auto-value-annotations:1.8.1",
        "com.google.guava:guava:30.1.1-jre",
        "com.google.inject:guice:5.0.1",
        "com.google.re2j:re2j:1.6",
        "com.google.truth:truth:1.1.3",
        "com.google.truth.extensions:truth-java8-extension:1.1.3",
        "info.picocli:picocli:4.6.1",
        "javax.inject:javax.inject:1",
        "junit:junit:4.13.2",
        "org.apache.avro:avro:1.10.2",
        "org.hsqldb:hsqldb:2.6.0",
        "org.hsqldb:sqltool:2.6.0",
        "org.mockito:mockito-core:3.11.1",
        "org.slf4j:slf4j-jdk14:1.7.32",
        "com.fasterxml.jackson.core:jackson-databind:2.12.2",
    ],
    fetch_sources = True,
    repositories = [
        "https://maven.google.com",
        "https://repo1.maven.org/maven2",
    ],
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_pkg",
    sha256 = "8a298e832762eda1830597d64fe7db58178aa84cd5926d76d5b744d6558941c2",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
    ],
)

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()
