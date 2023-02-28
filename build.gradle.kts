import dev.clojurephant.plugin.clojure.tasks.ClojureCompile

evaluationDependsOnChildren()

plugins {
    `java-library`
    id("dev.clojurephant.clojure") version "0.7.0"
}

allprojects {
    val proj = this

    group = "com.xtdb.labs"
    version = System.getenv("CORE2_VERSION") ?: "dev-SNAPSHOT"

    repositories {
        mavenCentral()
        maven { url = uri("https://repo.clojars.org/") }
    }

    if (plugins.hasPlugin("java-library")) {
        java {
            sourceCompatibility = JavaVersion.VERSION_11

            withSourcesJar()
            withJavadocJar()
        }

        tasks.javadoc {
            options {
                (this as CoreJavadocOptions).addStringOption("Xdoclint:none", "-quiet")
            }
        }

        tasks.test {
            useJUnitPlatform()

            jvmArgs = listOf(
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "-Dio.netty.tryReflectionSetAccessible=true"
            )

            exclude("integration", "kafka", "jdbc", "timescale", "s3", "slt", "docker")
        }

        if (plugins.hasPlugin("dev.clojurephant.clojure")) {
            dependencies {
                implementation("org.clojure", "clojure", "1.11.1")

                testRuntimeOnly("dev.clojurephant", "jovial", "0.4.1")
                nrepl("cider", "cider-nrepl", "0.28.6")
            }

            clojure {
                // disable `check` because it takes ages to start a REPL
                builds.forEach {
                    it.checkNamespaces.empty()
                }
            }

            tasks.clojureRepl {
                forkOptions.run {
                    jvmArgs = listOf(
                        "--add-opens=java.base/java.nio=ALL-UNNAMED",
                        "-Dio.netty.tryReflectionSetAccessible=true",
                        "-Djdk.attach.allowAttachSelf"
                    )
                }

                middleware.add("cider.nrepl/cider-middleware")
            }

            tasks.existing(ClojureCompile::class) {
                forkOptions.run {
                    jvmArgs = listOf(
                        "--add-opens=java.base/java.nio=ALL-UNNAMED",
                        "-Dio.netty.tryReflectionSetAccessible=true"
                    )
                }
            }
        }

        if (plugins.hasPlugin("maven-publish")) {
            extensions.configure(PublishingExtension::class) {
                publications.named("maven", MavenPublication::class) {
                    groupId = "com.xtdb.labs"
                    artifactId = "core2-${proj.name}"
                    version = proj.version.toString()
                    from(components["java"])

                    pom {
                        url.set("https://xtdb.com")

                        licenses {
                            license {
                                name.set("GNU Affero General Public License, Version 2 (AGPL 3)")
                                url.set("https://www.gnu.org/licenses/agpl-4.0.txt")
                            }
                        }
                        developers {
                            developer {
                                id.set("juxt")
                                name.set("JUXT")
                                email.set("hello@xtdb.com")
                            }
                        }
                        scm {
                            connection.set("scm:git:git://github.com/xtdb/core2.git")
                            developerConnection.set("scm:git:ssh://github.com/xtdb/core2.git")
                            url.set("https://xtdb.com")
                        }
                    }
                }

                repositories {
                    maven {
                        name = "ossrh"
                        val releasesRepoUrl = "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2"
                        val snapshotsRepoUrl = "https://s01.oss.sonatype.org/content/repositories/snapshots"
                        url = uri(if (!version.toString().endsWith("-SNAPSHOT")) releasesRepoUrl else snapshotsRepoUrl)

                        credentials {
                            username = project.properties["ossrhUsername"] as? String
                            password = project.properties["ossrhPassword"] as? String
                        }
                    }
                }

                extensions.configure(SigningExtension::class) {
                    useGpgCmd()
                    sign(publications["maven"])
                }
            }
        }
    }
}

dependencies {
    testImplementation(project(":api"))
    testImplementation(project(":wire-formats"))
    testImplementation(project(":core"))

    testImplementation(project(":http-server"))
    testImplementation(project(":http-client-clj"))
    testImplementation(project(":pgwire-server"))

    testImplementation(project(":modules:jdbc"))
    testImplementation(project(":modules:kafka"))
    testImplementation(project(":modules:s3"))

    testImplementation(project(":modules:bench"))
    testImplementation(project(":modules:c1-import"))
    testImplementation(project(":modules:datasets"))
    testImplementation(project(":modules:flight-sql"))

    testImplementation("org.clojure", "data.csv", "1.0.1")
    testImplementation("org.clojure", "tools.logging", "1.2.4")
    testImplementation("org.clojure", "tools.cli", "1.0.206")

    devImplementation("integrant", "repl", "0.3.2")
    devImplementation("ch.qos.logback", "logback-classic", "1.4.3")
    testImplementation("com.clojure-goes-fast", "clj-async-profiler", "1.0.0")
    testImplementation("org.postgresql", "postgresql", "42.5.0")
    testImplementation("cheshire", "cheshire", "5.11.0")
    testImplementation("org.xerial", "sqlite-jdbc", "3.39.3.0")
    testImplementation("org.clojure", "test.check", "1.1.1")
    testImplementation("org.apache.arrow", "flight-sql-jdbc-driver", "11.0.0")
}