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
    implementation("org.apache.lucene:lucene-core:9.2.0")
    implementation("org.apache.lucene:lucene-queryparser:9.0.0")
    implementation("org.apache.arrow:arrow-memory:8.0.0")
    implementation("org.apache.arrow:arrow-vector:8.0.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.12.5")
}

tasks.test {
    useJUnitPlatform()
}