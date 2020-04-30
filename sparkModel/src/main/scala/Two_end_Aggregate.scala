import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random
object Two_end_Aggregate {

  val conf = new SparkConf()
  conf.setAppName("Two-end_Aggregate").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val rddLines = sc.textFile("hdfs://")
  val worldContOne = rddLines.flatMap(_.split(" ")).map((_,1))

  //随机添加前缀
  val prefixRdd = worldContOne.map(worldContOne=>(Random.nextInt(10)+","+worldContOne._1,1))
  //局部聚合
  val localAggrRdd = prefixRdd.reduceByKey(_+_)

  //去掉前缀
  val removePrefixRdd = localAggrRdd.map(prefixRdd => (prefixRdd._1.split(",")(1), prefixRdd._2))
  //全局聚合
  val count = removePrefixRdd.reduceByKey(_+_)
}
