dependencies {
    api(project(":api"))
    api(project(":core"))

    api("org.apache.arrow", "arrow-vector", "11.0.0")
    api("org.apache.arrow", "flight-sql", "11.0.0")
}
