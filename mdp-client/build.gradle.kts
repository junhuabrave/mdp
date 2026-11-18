// Consumer-side library: stream subscription, recovery and books.
// Depends only on the wire contract (mdp-schema) and the Aeron client, never on the feed (mdp-core),
// so services such as mdp-api can consume the normalized stream without the feed's code.
val aeronVersion: String by project
dependencies {
    api(project(":mdp-schema"))
    api("io.aeron:aeron-client:$aeronVersion")
    testImplementation("io.aeron:aeron-driver:$aeronVersion")
}
