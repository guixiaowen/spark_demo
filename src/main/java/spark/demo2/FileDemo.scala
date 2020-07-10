package spark.demo2

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 分别是orderid,userid,payment,productid要求求出payment TOP N个，下面给出file1.txt、file2.txt、file3.txt文件，文件内容为：
 */
object FileDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("local").setMaster("local")
    val sparkContext = new SparkContext(sparkConf);

    var num = 0
    val fileRDD = sparkContext.textFile("/Users/guixiaowen/IdeaProjects/spark_demo/src/main/java/spark/demo2/file/", 3)
    val lineRDD = fileRDD.
//                    filter(x => x.length>0 && x.split(",") == 4).
                    map(x => (x.split(",")(2).toInt, x)).
//    lineRDD.foreach(x => println(" =========== : " + x) )
                    sortByKey(false).
                    top(5).
                    foreach(x => {num = num + 1; println(" num : " + num + " , data : " + x._1)})

  }

}
