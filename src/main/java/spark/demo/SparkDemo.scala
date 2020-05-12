package spark.demo

import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo {

  def main(args:Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sparkContext = new SparkContext(sparkConf)
    val distFile = sparkContext.textFile("/Users/guixiaowen/IdeaProjects/spark_demo/src/main/java/spark/demo/input/test.txt")
    val kvRdd = distFile.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    kvRdd.saveAsTextFile("/Users/guixiaowen/IdeaProjects/spark_demo/src/main/java/spark/demo/out")
    sparkContext.stop();

    var a = sparkContext.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b = a.map(x => (x.length, x))
    b.mapValues("x" + _ + "x").collect
    b.sortBy(x => x==2)



//    a = sparkContext.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
//    val b = sparkContext.parallelize(List(1,1,2,2,2,1,2,2,2), 3)
//    val c = b.zip(a)
//    val d = c.combineByKey(List(_), (x:List[String], y:String) => y :: x, (x:List[String], y:List[String]) => x ::: y)
//    d.collect
  }

}
