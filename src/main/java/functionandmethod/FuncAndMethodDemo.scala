package functionandmethod

class FuncAndMethodDemo {

}

object FuncAndMethodDemo {

  def methodDemo (x: Int) = x + 3
  val functionDemo = (x: Int) => x + 3

  def m2 (f: (Int, Int) => Int) = f(2, 6)

  def m1 (f: (Int, Int) => Int) = {
    f(2, 6)
  }

  val f1 = (x: Int, y: Int) => x + y
  val f2 = (x: Int, y: Int) => x * y

  def main(args: Array[String]): Unit = {
    val r1 = m1(f1)
    println(r1)
  }
}
