import org.apache.spark.{SparkConf, SparkContext}

/**
 * 拆分少量倾斜的key随机前缀，并扩容正常的key
 */
object SplitKeyToJoin {

  val conf = new SparkConf().setAppName("SplitKeyToJoin").setMaster("local[]")
  val sc = new SparkContext(conf)
  val lines = sc.textFile("hdfs://...")

  //Rdd中少数倾斜key，抽样10%
  val sampleRdd = lines.sample(false,0.1)
  /**
   * 1.统计key出现的次数
   * 2.key降序排序
   * 3.取出现次数最多的key
   */

   val sampleValueOneRdd = sampleRdd.map((_,1))
   val sampleCountRdd = sampleValueOneRdd.reduceByKey(_+_).map(keyValue=>(keyValue._2,keyValue._1))
   val skewdKeyRdd = sampleCountRdd.sortByKey(false)
   val skewdKey = skewdKeyRdd.take(3)
   

}
