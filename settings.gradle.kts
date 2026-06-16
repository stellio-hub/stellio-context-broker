// Declare the Kotlin plugin version in pluginManagement so it is consistent with the version
// declared in build.gradle.kts.
pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
    plugins {
        kotlin("jvm") version "2.3.21"
        kotlin("plugin.spring") version "2.3.21"
    }
}

rootProject.name = "stellio-context-broker"
include("shared", "api-gateway", "search-service", "subscription-service")
