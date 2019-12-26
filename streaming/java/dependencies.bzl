load("@rules_jvm_external//:defs.bzl", "maven_install")

def gen_streaming_java_deps():
    maven_install(
        name = "ray_streaming_maven",
        artifacts = [
            "com.alibaba:fastjson:1.2.50",
            "com.esotericsoftware:kryo:4.0.0",
            "com.esotericsoftware.minlog:minlog:1.2",
            "com.esotericsoftware:reflectasm:1.11.3",
            "com.google.guava:guava:27.0.1-jre",
            "com.github.davidmoten:flatbuffers-java:1.9.0.1",
            "com.google.protobuf:protobuf-java:3.8.0",
            "com.typesafe:config:1.3.2",
            "de.javakaffee:kryo-serializers:0.42",
            "de.ruedigermoeller:fst:2.57",
            "org.aeonbits.owner:owner:1.0.10",
            "org.apache.commons:commons-lang3:3.4",
            "org.apache.hadoop:hadoop-common:2.7.2",
            "org.apache.hadoop:hadoop-hdfs:2.7.2",
            "org.apache.logging.log4j:log4j-api:2.8.2",
            "org.apache.logging.log4j:log4j-core:2.8.2",
            "org.apache.logging.log4j:log4j-slf4j-impl:2.8.2",
            "org.mockito:mockito-core:3.0.0",
            "org.objenesis:objenesis:2.6",
            "org.slf4j:slf4j-api:1.7.12",
            "org.slf4j:slf4j-log4j12:1.7.25",
            "org.testng:testng:6.9.10",
        ],
        repositories = [
            "https://repo1.maven.org/maven2/",
        ],
    )
