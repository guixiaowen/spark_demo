package functionandmethod

object RepeatValDemo {

  def main(args: Array[String]): Unit ={
    printStrings("a", "b", "c")
  }

  def printStrings(strs: String*): Unit = {
    var i : Int = 0;
    for (str <- strs) {
      println("Arg value[" + i + "] = " + str)
      i = i + 1
    }
  }

}
