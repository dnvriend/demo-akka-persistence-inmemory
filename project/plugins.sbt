
resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "bintray-sbt-plugin-releases" at "http://dl.bintray.com/content/sbt/sbt-plugin-releases"

// to format scala source code
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")

// enable updating file headers eg. for copyright
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "1.5.1")

// to shoot akka / the jvm in the head
addSbtPlugin("io.spray" % "sbt-revolver" % "0.7.2")
