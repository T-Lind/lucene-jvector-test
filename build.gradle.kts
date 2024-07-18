plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.apache.lucene:lucene-core:9.2.0")
    implementation("org.apache.lucene:lucene-queryparser:9.0.0")
}

tasks.test {
    useJUnitPlatform()
}