plugins {
    application
    checkstyle
}

dependencies {
    checkstyle(libs.checkstyle)
    implementation(libs.aeron.archive)
    implementation(libs.agrona)
}

application {
    mainClass.set("com.aeroncookbook.archive.learning.MinimalArchiveHost")
    applicationDefaultJvmArgs = listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/java.util.zip=ALL-UNNAMED")
}
