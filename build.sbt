name := "InfluxBeam" 
version := "0.1.0" 
scalaVersion := "2.13.9"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.3.6"
libraryDependencies += "com.spotify" %% "scio-core" % "0.9.2"
libraryDependencies += "io.razem" %% "scala-influxdb-client" % "0.6.3"

//     implementation("io.razem:scala-influxdb-client_$scalaMajorVersion:0.6.3")
//     implementation("com.spotify:scio-core_$scalaMajorVersion:$scioVersionStrict")