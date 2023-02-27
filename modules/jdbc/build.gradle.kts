dependencies {
    api(project(":api"))
    api(project(":core"))

    api("org.clojure", "java.data", "1.0.95")
    api("pro.juxt.clojars-mirrors.com.github.seancorfield", "next.jdbc", "1.2.674")
    api("com.zaxxer", "HikariCP", "4.0.3")
}
