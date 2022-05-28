plugins {
    java
    checkstyle
}

group = "com.github.shoothzj"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    annotationProcessor("org.projectlombok:lombok:1.18.24")
    compileOnly("org.projectlombok:lombok:1.18.22")
    implementation("org.apache.pulsar:pulsar-client-admin-original:2.10.0")
    implementation("com.github.ben-manes.caffeine:caffeine:2.9.3")
    implementation("io.netty:netty-common:4.1.77.Final")
    implementation("com.google.guava:guava:31.1-jre")
    implementation("org.hibernate.validator:hibernate-validator:7.0.1.Final")
    implementation("org.apache.logging.log4j:log4j-core:2.17.2")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.2")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.22")
    testCompileOnly("org.projectlombok:lombok:1.18.22")
    testImplementation("org.apache.pulsar:pulsar-proxy:2.10.0") {
        exclude("org.slf4j", "slf4j-log4j12")
    }
    testImplementation("org.apache.pulsar:pulsar-broker:2.10.0") {
        exclude("org.slf4j", "slf4j-log4j12")
    }
    testImplementation("org.apache.bookkeeper:bookkeeper-server:4.14.4") {
        exclude("org.slf4j", "slf4j-log4j12")
    }
    testImplementation("org.assertj:assertj-core:3.22.0")
    testImplementation("org.glassfish:jakarta.el:4.0.2")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.2")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}