plugins {
    java
    checkstyle
}

group = "com.github.shoothzj"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val junitVersion = "5.9.0"
val lombokVersion = "1.18.24"
val log4jVersion = "2.18.0"
val pulsarVersion = "2.10.1"

dependencies {
    annotationProcessor("org.projectlombok:lombok:$lombokVersion")
    compileOnly("org.projectlombok:lombok:$lombokVersion")
    implementation("org.apache.pulsar:pulsar-client-admin-original:$pulsarVersion")
    implementation("com.github.ben-manes.caffeine:caffeine:2.9.3")
    implementation("io.netty:netty-common:4.1.83.Final")
    implementation("com.google.guava:guava:31.1-jre")
    implementation("org.hibernate.validator:hibernate-validator:7.0.5.Final")
    implementation("org.apache.logging.log4j:log4j-core:$log4jVersion")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
    testAnnotationProcessor("org.projectlombok:lombok:$lombokVersion")
    testCompileOnly("org.projectlombok:lombok:$lombokVersion")
    testImplementation("org.apache.pulsar:pulsar-proxy:$pulsarVersion") {
        exclude("org.slf4j", "slf4j-log4j12")
    }
    testImplementation("org.apache.pulsar:pulsar-broker:$pulsarVersion") {
        exclude("org.slf4j", "slf4j-log4j12")
    }
    testImplementation("org.apache.bookkeeper:bookkeeper-server:4.14.4") {
        exclude("org.slf4j", "slf4j-log4j12")
    }
    testImplementation("org.assertj:assertj-core:3.23.1")
    testImplementation("org.glassfish:jakarta.el:4.0.2")
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}