
load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")

def ray_on_yarn_maven_install():
    maven_install(
        name = "ray_on_yarn_maven",
        artifacts = [
            "org.apache.hadoop:hadoop-yarn-common:2.8.0",
            "org.apache.hadoop:hadoop-common:2.8.0",
            "org.apache.hadoop:hadoop-yarn-client:2.8.0",
            "org.testng:testng:6.11",
        ],
        repositories = [
            "https://jcenter.bintray.com",
            "https://maven.google.com",
            "https://repo1.maven.org/maven2",
        ]
    )
