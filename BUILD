load("@rules_jvm_external//:defs.bzl", "artifact", "maven_install")

java_binary(
    name = "mongodump_s3_uploader",
    main_class = "com.example.MongodumpS3Uploader",
    srcs = glob(["src/main/java/com/example/MongodumpS3Uploader.java"]),
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

java_binary(
    name = "async_mongodump_s3_uploader",
    main_class = "com.example.AsyncMongodumpS3Uploader",
    srcs = glob(["src/main/java/com/example/AsyncMongodumpS3Uploader.java"]),
    deps = [
        "@maven//:software_amazon_awssdk_s3",
        "@maven//:software_amazon_awssdk_sdk_core",
        "@maven//:software_amazon_awssdk_auth",
        "@maven//:software_amazon_awssdk_regions",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:software_amazon_awssdk_netty_nio_client",
        "@maven//:software_amazon_awssdk_http_client_spi",
        "@maven//:io_netty_netty_all",   
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:org_slf4j_slf4j_simple",
    ],
)

