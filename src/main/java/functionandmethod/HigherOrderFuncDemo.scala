package functionandmethod

object HigherOrderFuncDemo {

  def main(args: Array[String]): Unit = {
//    println(apply((x: Any) => "[" + x.toString + "]", 10))

    println(apply(_.toString, 10))
  }

  def apply(f: Int => String, v: Int) = f(v)

//  def layout[U] (x: U) = "[" + x.toString + "]"

}
