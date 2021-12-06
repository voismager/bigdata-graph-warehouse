plugins {
    kotlin("jvm") version "1.6.0"
    id("org.springframework.boot") version "2.6.0"
    id("org.jetbrains.kotlin.plugin.spring") version "1.6.0"
    id("io.spring.dependency-management") version "1.0.11.RELEASE"
    id("com.google.cloud.tools.jib") version "3.1.4"
}

group = "org.fluffytiger"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib:1.6.0")

    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("com.orientechnologies:orientdb-client:3.2.3")
    implementation("org.springframework.data:spring-data-redis")
    implementation("redis.clients:jedis:3.7.0")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

tasks.test {
    useJUnitPlatform()
}

jib {
    to {
        image = "fluffytiger/redis-initializer"
        tags = setOf("$version")
    }
}