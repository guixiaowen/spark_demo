package spark.demo

import org.apache.spark.{SparkConf, SparkContext}

object GroupByDemo extends App {

  val sparkConf = new SparkConf().setAppName("local").setMaster("local")
  val sparkContext = new SparkContext(sparkConf);
//  val demo = sparkContext.parallelize(List(("bc", 2), ("dc", 3), ("ef", 4), ("gh", 3), ("bc", 4), ("gh", 3)), 1);
//  println(demo.partitions.map(x => x))
//  val demo2 = demo.groupByKey()
//  demo2.top(4)

  val demo = sparkContext.parallelize(List("2", "4", "1", "6", "9"))
  demo.map(x => x.toInt).reduce(_ + _)

}
