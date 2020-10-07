var a = -1
lazy val x = if (a < 0) {
  println(s"${a} < 0")
  "hello"
} else {
  println(s"${a} > 0")
  "world"
}

a = 1
println(x)
println(x)
println(x)
println(x)
println(x)
