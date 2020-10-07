/**
 * Author: zhangying14
 * Date: 2020/9/25 16:23
 * Package: 
 * Description:
 *
 */
object Test extends App {
  case class A(i: Int)
  case class B(c: String)
  val a = A(1)
  val b = A(2)
  val c = B("hello")
  val d = B("world")
  val e = a

  println(a, b, c, d)

}
