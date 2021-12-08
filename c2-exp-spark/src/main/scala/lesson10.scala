import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkContext}

import java.io.File

object Lesson10 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val sc = new SparkContext("local", "error")
    //sc.textFile("hdfs:///test/input").filter(_.contains("ERROR")).count()
    sc.makeRDD(1 to 20, 4)
      .foreachPartition(itr => {
        print("partition:")
        itr.foreach(x => print(x + ","))
        println("")
      })

    sc.makeRDD(1 to 20, 4)
      .mapPartitions(itr => {
        itr.map(x=>x*x)
      })
      .foreachPartition(itr => {
        print("partition:")
        itr.foreach(x => print(x + ","))
        println("")
      })
    sc.makeRDD(1 to 20, 4)
      .mapPartitionsWithIndex((i,itr) => {
        itr.map(x=>i+"-"+x)
      })
      .foreachPartition(itr => {
        print("partition:")
        itr.foreach(x => print(x + ","))
        println("")
      })
    val a = 1 to 20
    val b = 'a' to 't'
    val rdd = sc.makeRDD(a.zip(b))
    rdd.partitionBy(new RangePartitioner(4,rdd))
      //.partitionBy(new HashPartitioner(4))
      .mapPartitionsWithIndex((i,itr) => {
        itr.map(x=>i+"-"+x)
      })
      .foreachPartition(itr => {
        itr.foreach(x => print(x + ","))
        println("")
      })
    println("分区相关算子：coalesce/repartition/repartitionAndSortWithinPartitions")
    rdd.repartition(4)
      //.coalesce(2)
      //.coalesce(5)
      //.repartitionAndSortWithinPartitions(new HashPartitioner(4))
      .mapPartitionsWithIndex((i,itr) => {
        itr.map(x=>i+"-"+x)
      })
      .foreachPartition(itr => {
        itr.foreach(x => print(x + ","))
        println("")
      })
    println("分区相关算子：pipe/glom")
    rdd.repartition(4)
      .pipe("cat")
      .glom()
      .mapPartitionsWithIndex((i,itr) => {
        itr.map(x=>i+"-"+x.toList)
      })
      .foreachPartition(itr => {
        itr.foreach(x => print(x + ","))
        println("")
      })

  }
}

object TestScala{
  def main(args: Array[String]): Unit = {
    val a = 1 to 20
    val b = 'a' to 't'
    println(a.zip(b))

  }
}