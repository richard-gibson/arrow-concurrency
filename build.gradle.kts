import org.jlleitschuh.gradle.ktlint.reporter.ReporterType

plugins {
  id("org.jetbrains.kotlin.jvm").version("1.3.41")
  id("org.jlleitschuh.gradle.ktlint").version("8.1.0")
  id("org.jetbrains.kotlin.kapt") version "1.3.41"
}

val arrowVersion = "0.9.1-SNAPSHOT"

val kotlintestVersion = "3.3.1"

repositories {
  jcenter()
  mavenCentral()
  maven(url = "https://oss.jfrog.org/oss-snapshot-local/")
}

dependencies {
  implementation(kotlin("stdlib-jdk8"))
  implementation(kotlin("reflect"))

  implementation(group = "io.arrow-kt", name = "arrow-core", version = arrowVersion)
  implementation(group = "io.arrow-kt", name = "arrow-syntax", version = arrowVersion)
  implementation(group = "io.arrow-kt", name = "arrow-fx", version = arrowVersion)

  kapt(group = "io.arrow-kt", name = "arrow-meta", version = arrowVersion)

  testImplementation(group = "io.arrow-kt", name = "arrow-test", version = arrowVersion)
  testImplementation(group = "io.kotlintest", name = "kotlintest-core", version = kotlintestVersion)
  testImplementation(group = "io.kotlintest", name = "kotlintest-assertions-arrow", version = kotlintestVersion)
}

ktlint {
  verbose.set(true)
  outputToConsole.set(true)
  coloredOutput.set(true)
  reporters.set(setOf(ReporterType.CHECKSTYLE, ReporterType.JSON))
}
