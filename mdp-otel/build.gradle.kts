val otelVersion: String by project
val otelRuntimeVersion: String by project
dependencies {
    api(project(":mdp-core"))
    implementation(platform("io.opentelemetry:opentelemetry-bom:$otelVersion"))
    implementation("io.opentelemetry:opentelemetry-sdk")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp")
    implementation("io.opentelemetry.instrumentation:opentelemetry-runtime-telemetry:$otelRuntimeVersion")
    testImplementation("io.opentelemetry:opentelemetry-sdk-testing")
}
