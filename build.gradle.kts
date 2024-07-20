plugins {
    id("java")
}

group = "org.tlind"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.apache.lucene:lucene-core:9.9.0")
    implementation("org.apache.lucene:lucene-queryparser:9.0.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.12.5")
    implementation("com.fasterxml.jackson.core:jackson-core:2.12.5")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.12.5")
}

tasks.test {
    useJUnitPlatform()
}

// Improves performance of Java vector incubator API, default is preferredBitSize=128
tasks.withType<JavaExec> {
    jvmArgs("--add-modules", "jdk.incubator.vector")
}