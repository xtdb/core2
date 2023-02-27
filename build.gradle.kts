import dev.clojurephant.plugin.clojure.tasks.ClojureCheck
import dev.clojurephant.plugin.clojure.tasks.ClojureNRepl

group = "core2"
version = System.getenv("CORE2_VERSION") ?: "dev-SNAPSHOT"

plugins {
    `java-library`
    id("dev.clojurephant.clojure") version "0.7.0"
}

repositories {
    mavenCentral()
    maven { url = uri("https://repo.clojars.org/") }
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "dev.clojurephant.clojure")
}

allprojects {
    java.sourceCompatibility = JavaVersion.VERSION_11

    repositories {
        mavenCentral()
        maven { url = uri("https://repo.clojars.org/") }
    }

    dependencies {
        implementation("org.clojure", "clojure", "1.11.1")

        testRuntimeOnly("dev.clojurephant", "jovial", "0.4.1")
        nrepl("cider", "cider-nrepl", "0.28.6")
    }

    tasks.test {
        useJUnitPlatform()
    }

    clojure {
        // disable `check` because it takes ages to start a REPL
        builds.forEach {
            it.checkNamespaces.empty()
        }
    }
}

val clojureRepl by tasks.existing(ClojureNRepl::class) {
    forkOptions.run {
        jvmArgs = listOf(
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "-Dio.netty.tryReflectionSetAccessible=true",
            "-Djdk.attach.allowAttachSelf"
        )
    }

    middleware.add("cider.nrepl/cider-middleware")
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
