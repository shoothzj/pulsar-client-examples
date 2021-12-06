plugins {
    java
}

group = "com.github.shoothzj"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    annotationProcessor("org.projectlombok:lombok:1.18.22")
    compileOnly("org.projectlombok:lombok:1.18.22")
    implementation("org.apache.pulsar:pulsar-client-admin:2.9.0")
    implementation("com.github.ben-manes.caffeine:caffeine:3.0.5")
    implementation("io.netty:netty-common:4.1.70.Final")
    implementation("com.google.guava:guava:31.0.1-jre")
    implementation("org.hibernate.validator:hibernate-validator:7.0.1.Final")
    testImplementation("org.glassfish:jakarta.el:4.0.2")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.14.1")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.2")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}