name := "jodie"
organization := "com.github.mrpowers"

version := "0.0.3"

crossScalaVersions := Seq("2.12.17", "2.13.10")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1" % "provided"

libraryDependencies += "io.delta" %% "delta-core" % "2.1.0" % "provided"
libraryDependencies += "com.github.mrpowers" %% "spark-daria" % "1.2.3" % "test"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % "test"
libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.7.1" % "test"

// test suite settings
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

// deploy stuff
credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/MrPowers/jodie"))
developers ++= List(
  Developer("MrPowers", "Matthew Powers", "@MrPowers", url("https://github.com/MrPowers"))
)
scmInfo := Some(ScmInfo(url("https://github.com/MrPowers/jodie"), "git@github.com:MrPowers/jodie.git"))

updateOptions := updateOptions.value.withLatestSnapshots(false)

publishMavenStyle := true

publishTo := sonatypePublishToBundle.value

Global / useGpgPinentry := true
