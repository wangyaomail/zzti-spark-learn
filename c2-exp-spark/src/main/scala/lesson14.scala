import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HConstants, HTableDescriptor, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.sql.DriverManager
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

// 1.HBase测试
object HBaseWriteTest{
  val logger = Logger.getLogger("org")
  logger.setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    //1. 在 Spark 2.x 中建议使用以下方式获取sc对象
    val sparkSession = SparkSession.builder()
      .config("spark.hadoop.validateOutputSpecs","false")
      .appName("HBase")
      .master("local[*]")
      .getOrCreate()
    //2. 创建 SparkContext 对象
    val sc = sparkSession.sparkContext
    //3. 获取 hadoopConfiguration 配置并设置HBase相关属性
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,"spark")
    sc.hadoopConfiguration.set(HConstants.ZOOKEEPER_QUORUM, "192.168.17.150:2181")
    //4. 根据Hadoop的配置创建MapReduce的Job对象
    val job = Job.getInstance(sc.hadoopConfiguration)
    //设置输出的 key 的类型
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    //设置输出的 VALUE 的类型
    job.setOutputValueClass(classOf[Result])
    //设置输出格式的类型
    //ImmutableBytesWritable一般作为 RowKey的类型
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    //5. 创建表的名字对象
    val tableName = TableName.valueOf("spark")
    //6. 如果表不存在，则创建一个，存在则不创建
    val admin = ConnectionFactory.createConnection(sc.hadoopConfiguration).getAdmin
    if(!admin.tableExists(tableName)){
      //6.1 创建表的描述对象
      val tableDesc = new HTableDescriptor(tableName)
      //6.2 添加列族信息
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes))
      //6.3 创建表
      admin.createTable(tableDesc)
    }
    //7. 定义本地数据
    val rdd = sc.makeRDD(List(
      ("1001","王磊","17"),
      ("1002","张京","18"),
      ("1003","张彦","17"),
      ("1004","刘杰","18")))

    //8. 转换 RDD 为指定的数据类型
    val hbaseRDD: RDD[(ImmutableBytesWritable, Put)] = rdd.map(value => {
      //8.1 根据 rowkey 创建put对象
      val put = new Put(Bytes.toBytes(value._1))
      //8.2 添加该rowkey所对应的列的信息：列名和数据值
      put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(value._2))
      put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(value._3))
      //8.3 返回rowkey对象和put对象
      (new ImmutableBytesWritable(Bytes.toBytes(value._1)), put)
    })
    //9. 保存数据到HBase中
    hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sc.stop()
  }
}

object HBaseReadTest{
  val logger = Logger.getLogger("org")
  logger.setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    //1. 在 Spark 2.x 中建议使用以下方式获取sc对象
    val sparkSession = SparkSession.builder().appName("HBase").master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    //2. 创建 HBase配置对象
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.17.150:2181") //设置zookeeper集群，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "zzti")
    //3. 从HBase读取数据形成RDD
    val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],//table
      classOf[ImmutableBytesWritable],//hbase table rowkey
      classOf[Result])//resultset
    //4. 对hbaseRDD进行处理
    rdd.foreach {
      case (_, res) =>
        for (cell <- res.rawCells()){
          print("RowKey: " + new String(CellUtil.cloneRow(cell)) + ", ");
          print("时间戳: " + cell.getTimestamp() + ", ");
          print("列名: " + new String(CellUtil.cloneQualifier(cell)) + ", ");
          println("值: " + new String(CellUtil.cloneValue(cell)));
        }
    }
    sc.stop()
  }
}


// 2. 累加器
object accSysTest{
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val conf = new SparkConf().setAppName("累加器").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //val data = sc.textFile("in/TeamViewer15_Logfile_OLD.log",6)
    val data = sc.textFile("in/TeamViewer15_Logfile_OLD.log",6)
    //创建一个系统自带的累加器
    val errors = sc.longAccumulator("acc")
    //向sc注册累加器
    //sc.register(errors)
    val result = data.map(line => {
      if(line.contains("Error")){
        errors.add(1)
      }
      (line,1)
    })
    println(result.count())
    println(errors.value)
    sc.stop()
  }
}

case class SumAandB(a:Long,b:Long)
class MyAccumulator extends AccumulatorV2[SumAandB,SumAandB]{
  private var a:Long = 0
  private var b:Long = 0
  //只有a和b同时为0，则累加器为0
  override def isZero: Boolean = a==0 && b ==0
  //复制累加器
  override def copy(): AccumulatorV2[SumAandB, SumAandB] = {
    val acc = new MyAccumulator
    acc.a = this.a
    acc.b = this.b
    acc
  }
  //重置累加器
  override def reset(): Unit = {
    this.a = 0
    this.b = 0
  }
  //用累加器汇总结果
  override def add(v: SumAandB): Unit = {
    a += v.a
    b += v.b
  }
  //不同分区的累加器的合并
  override def merge(other: AccumulatorV2[SumAandB, SumAandB]): Unit = {
    other match {
      case o:MyAccumulator =>{
        a += o.b
        b += o.b
      }
    }
  }
  //Driver端调用结果的时候的返回值
  override def value: SumAandB = {
    SumAandB(a,b)
  }
}

object accUserTest{
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("累加器").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //创建累加器对象
    val acc = new MyAccumulator
    //向sc注册累加器
    sc.register(acc,"Count")
    val data = sc.textFile("in/acc.csv")
      .filter(_.split(",")(0) != "A")
      .map(v=>{
        val fields = v.split(",")
        acc.add(SumAandB(fields(0).toLong,fields(1).toLong))
      }).count()
    println(acc.value)
    sc.stop()
  }
}


// 3. checkpoint
object checkpoint{
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val conf = new SparkConf().setAppName("ChickPoint").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //设置检查点目录，可以是HDFS等文件系统
    sc.setCheckpointDir(".checkpoint")
    val rdd1 = sc.makeRDD(Array("Spark"))
    val rdd2_source = rdd1.map(_+":"+System.currentTimeMillis())
    val rdd2_cache = rdd1.map(_+":"+System.currentTimeMillis())
    val rdd2_check = rdd1.map(_+":"+System.currentTimeMillis())
    println("-----------没有设置 Checkpoint-----------")
    rdd2_source.foreach(println)
    rdd2_source.foreach(println)
    rdd2_source.foreach(println)
    println("-----------设置 Cache 后-----------")
    rdd2_cache.cache()
    rdd2_cache.foreach(println)
    rdd2_cache.foreach(println)
    rdd2_cache.foreach(println)
    println("-----------设置 Checkpoint 后-----------")
    rdd2_check.checkpoint()
    rdd2_check.foreach(println)
    rdd2_check.foreach(println)
    rdd2_check.foreach(println)
    rdd2_check.foreach(println)
    rdd2_check.foreach(println)
    sc.stop()
  }
}