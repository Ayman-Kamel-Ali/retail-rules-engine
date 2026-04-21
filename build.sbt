name := "retail-rules-engine"
version := "1.0"
scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "org.xerial"             %  "sqlite-jdbc"                % "3.44.1.0",
  "org.postgresql"         %  "postgresql"                 % "42.7.2",
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"
)