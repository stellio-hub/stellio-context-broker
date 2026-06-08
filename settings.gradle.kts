// Declare the Kotlin plugin version in pluginManagement so it is consistent with the version
// declared in build.gradle.kts and resolves from the milestone repo (needed for Spring Boot RC).
pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
        maven { url = uri("https://repo.spring.io/milestone") }
    }
    plugins {
        kotlin("jvm") version "2.3.20"
        kotlin("plugin.spring") version "2.3.20"
    }
}

rootProject.name = "stellio-context-broker"
include("shared", "api-gateway", "search-service", "subscription-service")
