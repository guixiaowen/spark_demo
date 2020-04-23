package functionandmethod

class FuncAndMethodDemo2 {

}

object FuncAndMethodDemo2 {

  def ttt (f: Int => Int): Unit = {
    val r = f(10)
    println(r)
  }

  val f0 = (x : Int) => x * x

  def m0 (x : Int) : Int = {
    x * 10
  }

  val f1 = m0 _

  def main(args: Array[String]): Unit = {
    ttt(f0)

    ttt(m0 _)

    ttt(m0)

    ttt(x => m0(x))
  }
}
