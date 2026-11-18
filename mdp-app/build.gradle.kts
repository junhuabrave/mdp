plugins { application }
val aeronVersion: String by project
dependencies {
    implementation(project(":mdp-core"))
    implementation(project(":mdp-cme"))
    implementation(project(":mdp-euronext"))
    implementation(project(":mdp-otel"))
    implementation("io.aeron:aeron-driver:$aeronVersion")
    implementation("io.aeron:aeron-archive:$aeronVersion")
}
val hotPathJvmArgs = listOf(
    "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "-XX:+UseZGC", "-XX:+AlwaysPreTouch", "-Xms1g", "-Xmx1g",
    "-Dagrona.disable.bounds.checks=true",
    "-Daeron.term.buffer.sparse.file=false",
)
application {
    mainClass.set("io.mdp.app.Main")
    applicationDefaultJvmArgs = hotPathJvmArgs
}
tasks.register<JavaExec>("benchmark") {
    group = "verification"
    description = "Runs the in-process latency benchmark"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("io.mdp.app.bench.LatencyBenchmark")
    jvmArgs(hotPathJvmArgs)
    args(providers.gradleProperty("benchArgs").map { it.split(" ") }.getOrElse(emptyList()))
}
