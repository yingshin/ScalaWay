//val lookupKeys = Array("a", "b", "c")

val indices: Array[Array[String]] = Array(
  Array("a", "b", "c"),
  Array("d", "e")
)

def foo(lookupKeys: Array[String]) = {
  indices.find(i => i.forall(j => lookupKeys.contains(j)))
}

println(foo(Array("a", "b", "c")))
println(foo(Array("a", "b", "c")).nonEmpty)
println(foo(Array("a")))
println(foo(Array("a")).nonEmpty)
println(foo(Array("a", "b", "c", "d")))
println(foo(Array("a", "b", "c", "d")).nonEmpty)
