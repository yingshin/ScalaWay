import java.io.File

new File("/tmp/a.txt").getName

val m: Map[String, Any] = Map(
  "a" -> 1,
  "b" -> 1.23,
  "c" -> "hello world",
  "d" -> None,
  "e" -> null
)

val s = m.values.mkString(",")
s.split(",")

"1,2,3,,4,,5".split(",")


