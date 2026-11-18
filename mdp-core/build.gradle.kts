val aeronVersion: String by project
val hdrHistogramVersion: String by project
dependencies {
    api(project(":mdp-client"))
    api("io.aeron:aeron-client:$aeronVersion")
    api("org.hdrhistogram:HdrHistogram:$hdrHistogramVersion")
    implementation("io.aeron:aeron-driver:$aeronVersion")
    testImplementation("io.aeron:aeron-driver:$aeronVersion")
}
