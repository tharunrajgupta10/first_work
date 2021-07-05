name := "work1"

version := "0.1"

scalaVersion := "2.13.6"


// https://mvnrepository.com/artifact/org.springframework.boot/spring-boot-starter-web
libraryDependencies += "org.springframework.boot" % "spring-boot-starter-web" % "2.5.2"
// https://mvnrepository.com/artifact/org.springframework.boot/spring-boot-starter-test
libraryDependencies += "org.springframework.boot" % "spring-boot-starter-test" % "2.5.2"
// https://mvnrepository.com/artifact/io.springfox/springfox-swagger-ui
libraryDependencies += "io.springfox" % "springfox-swagger-ui" % "3.0.0"
// https://mvnrepository.com/artifact/io.springfox/springfox-swagger2
libraryDependencies += "io.springfox" % "springfox-swagger2" % "3.0.0"


// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-core
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.12.3"
// https://mvnrepository.com/artifact/org.json/json
libraryDependencies += "org.json" % "json" % "20210307"



//------sbt dependencies
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.8.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.8.0"

//------logging depenencies
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.0-alpha1"
// https://mvnrepository.com/artifact/org.slf4j/slf4j-log4j12
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "2.0.0-alpha1"

// https://mvnrepository.com/artifact/org.springframework/spring-core
libraryDependencies += "org.springframework" % "spring-core" % "5.3.8"
// https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.13"

//io confluent resolve repository
// io confluent packages doesn't exist on Maven2 Repository server where sbt searches,
// so we need to tell the sbt that these pacakges locates at this server
resolvers += "confluent" at "https://packages.confluent.io/maven/"

//------------avro dependencies--------------
// https://mvnrepository.com/artifact/org.apache.avro/avro
libraryDependencies += "org.apache.avro" % "avro" % "1.10.2"
// https://mvnrepository.com/artifact/io.confluent/kafka-avro-serializer
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "6.2.0"
// https://mvnrepository.com/artifact/io.confluent/kafka-schema-registry-client
libraryDependencies += "io.confluent" % "kafka-schema-registry-client" % "6.2.0"
// https://mvnrepository.com/artifact/io.confluent/kafka-streams-avro-serde
libraryDependencies += "io.confluent" % "kafka-streams-avro-serde" % "6.2.0"



//leverage java 8
javacOptions ++= Seq("-source","1.8","-target","1.8","-Xlint")
scalacOptions :=Seq("-target:jvm-1.8")

