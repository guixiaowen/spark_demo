package spark.demo

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo {

  def main(args:Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sparkContext = new SparkContext(sparkConf)
    val distFile = sparkContext.textFile("/Users/guixiaowen/IdeaProjects/spark_demo/src/main/java/spark/demo/input/test.txt")

    val distFile2 = sparkContext.hadoopFile("/Users/guixiaowen/IdeaProjects/spark_demo/src/main/java/spark/demo/input/test.txt", classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 2)
    // 高阶函数
    // 等价于：x => x.split(" ") ｜
    //
    distFile.flatMap(_.split(" "))
    // distFile.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).saveAsTextFile("/Users/guixiaowen/IdeaProjects/spark_demo/src/main/java/spark/demo/out")

    sparkContext.stop();
  }

}
