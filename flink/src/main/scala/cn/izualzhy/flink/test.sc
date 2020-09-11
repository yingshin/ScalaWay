val s1 = "bootstrap.server"
val s2 = "topic"
val s3 = "primary_key"
val m = Map(
  s1 -> "1.2.3.4",
  s2 -> "ufo",
  "primary_key" -> "uid",
  "a" -> 1
)


Map("1"->"2", "3"->"4") ++ m.get(s3).map("connector.kafka.key" -> _.toString)

val s = "uid"
s.split("\\.").tail
s.split("\\.").last
