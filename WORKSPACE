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
        "com.google.auto.value:auto-value:1.8.1",
        "com.google.auto.value:auto-value-annotations:1.8.1",
        "com.google.code.findbugs:jsr305:3.0.2",
        "com.google.guava:guava:30.1.1-jre",
        "com.google.inject:guice:5.0.1",
        "com.google.re2j:re2j:1.6",
        "com.google.truth:truth:1.1.3",
        "info.picocli:picocli:4.6.1",
        "javax.inject:javax.inject:1",
        "junit:junit:4.13.2",
        "org.apache.avro:avro:1.10.2",
        "org.hsqldb:hsqldb:2.6.0",
        "org.hsqldb:sqltool:2.6.0",
        "org.mockito:mockito-core:3.11.1",
    ],
    fetch_sources = True,
    repositories = [
        "https://maven.google.com",
        "https://repo1.maven.org/maven2",
    ],
)
