// Timeline v1: one keyed state both devices of a link keep, synced over the
// relay. A separate artifact, so apps that never keep a timeline do not ship
// it; like the core library, no runtime dependency beyond the Kotlin
// standard library.
plugins {
    kotlin("jvm")
    `java-library`
    `maven-publish`
}

group = "cloud.zerobyte"
version = "1.2.0"

kotlin {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_11)
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
    withSourcesJar()
}

dependencies {
    api(project(":zerobyte"))
    testImplementation(kotlin("test-junit5"))
    testImplementation("org.json:json:20260814")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    testLogging { events("passed", "skipped", "failed") }
}

publishing {
    publications {
        create<MavenPublication>("release") {
            artifactId = "zerobyte-timeline"
            from(components["java"])
            pom {
                name.set("ZeroByte Timeline")
                description.set("One keyed state both devices of a ZeroByte link keep, synced over the relay (Timeline v1).")
                url.set("https://docs.zerobyte.cloud/sdk/android/")
                licenses {
                    license {
                        name.set("Apache-2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
                developers {
                    developer {
                        id.set("szymonmichalik")
                        name.set("Szymon Michalik")
                        url.set("https://github.com/szymonmichalik")
                    }
                }
                scm {
                    url.set("https://github.com/zerobyte-cloud/zerobyte-android-sdk")
                    connection.set("scm:git:https://github.com/zerobyte-cloud/zerobyte-android-sdk.git")
                    developerConnection.set("scm:git:ssh://git@github.com/zerobyte-cloud/zerobyte-android-sdk.git")
                }
            }
        }
    }
}
