import org.apache.spark.{SparkConf, SparkContext}

object ToMapJoin {
  val conf = new SparkConf().setMaster("local[]").setAppName("ToMapJoin")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("hdfs://...")
  val blackListData = lines.filter(_.contains("cao")).collect()
  final val blackList = sc.broadcast(blackListData)
}
