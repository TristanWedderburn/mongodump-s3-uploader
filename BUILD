load("@rules_jvm_external//:defs.bzl", "artifact", "maven_install")

java_binary(
    name = "mongodump_s3_uploader",
    main_class = "com.example.MongodumpS3Uploader",
    srcs = glob(["src/main/java/com/example/*.java"]),
    deps = [
        "@maven//:software_amazon_awssdk_s3",
        "@maven//:software_amazon_awssdk_sdk_core",
        "@maven//:software_amazon_awssdk_auth",
        "@maven//:software_amazon_awssdk_regions",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:org_slf4j_slf4j_simple",
    ],
)

