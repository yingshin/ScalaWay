class S(s: String) {
  override def toString: String = s"|${s}|"
}

trait A {
  val s: S

  def bar() = println(s)
}

class B(val s: S) extends A {
  def foo() = {
    println(s)
  }
}

class C(override val s: S) extends B(s) {
//  class C(s: S) extends B(s) {
  def h() = println(s)
}

val c = new C(new S("hello world"))
c.h()
c.foo()
c.bar()