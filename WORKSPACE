# This file marks the root of the Bazel workspace.
# See MODULE.bazel for external dependencies setup.

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Import rules_jvm_external for Java dependencies
http_archive(
    name = "rules_jvm_external",
    urls = ["https://github.com/bazelbuild/rules_jvm_external/archive/4.3.zip"],
    strip_prefix = "rules_jvm_external-4.3",
)

load("@rules_jvm_external//:defs.bzl", "maven_install")

# Maven dependencies
maven_install(
    artifacts = [
        "software.amazon.awssdk:s3:2.20.12",
        "software.amazon.awssdk:sdk-core:2.20.12",
        "software.amazon.awssdk:auth:2.20.12",
        "software.amazon.awssdk:regions:2.20.12",
        "org.apache.commons:commons-lang3:3.12.0",
        "org.slf4j:slf4j-api:1.7.30",
        "org.slf4j:slf4j-simple:1.7.30"
    ],
    repositories = [
        "https://repo1.maven.org/maven2",
    ],
)
