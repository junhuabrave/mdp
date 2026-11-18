// Shared build conventions for all modules.
// Any module with src/main/sbe/*.xml gets an SBE code generation task wired into compileJava.

val sbeVersion: String by project
val junitVersion: String by project

subprojects {
    apply(plugin = "java-library")

    group = "io.mdp"
    version = "0.1.0-SNAPSHOT"

    repositories { mavenCentral() }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.release.set(21)
        options.compilerArgs.addAll(listOf("-Xlint:all,-processing,-serial,-this-escape"))
    }

    dependencies {
        "testImplementation"(platform("org.junit:junit-bom:$junitVersion"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        jvmArgs("--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
        testLogging { events("failed"); exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL }
    }

    val sbeDir = file("src/main/sbe")
    if (sbeDir.isDirectory) {
        val sbeTool = configurations.create("sbeTool")
        dependencies { add("sbeTool", "uk.co.real-logic:sbe-tool:$sbeVersion") }

        val generatedDir = layout.buildDirectory.dir("generated-src/sbe")
        val generateSbe = tasks.register<JavaExec>("generateSbe") {
            group = "build"
            description = "Generates SBE codecs from src/main/sbe/*.xml"
            val schemas = fileTree(sbeDir) { include("*.xml") }
            inputs.files(schemas)
            outputs.dir(generatedDir)
            mainClass.set("uk.co.real_logic.sbe.SbeTool")
            classpath = sbeTool
            jvmArgs("--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED")
            systemProperty("sbe.output.dir", generatedDir.get().asFile.absolutePath)
            systemProperty("sbe.target.language", "Java")
            systemProperty("sbe.validation.stop.on.error", "true")
            systemProperty("sbe.validation.warnings.fatal", "false")
            systemProperty("sbe.java.generate.interfaces", "true")
            args(schemas.files.map { it.absolutePath }.sorted())
            doFirst { delete(generatedDir) }
        }
        extensions.configure<SourceSetContainer> {
            named("main") { java.srcDir(generateSbe) }
        }
        tasks.withType<JavaCompile>().configureEach {
            // generated code triggers some lint warnings we do not control
            options.compilerArgs.add("-Xlint:-cast,-fallthrough")
        }
    }
}
