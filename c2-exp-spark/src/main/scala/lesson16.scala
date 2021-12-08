import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.io.File

object lesson16_sql2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession
      .builder()
      .appName("sql1")
      .master("local[*]").getOrCreate()
    //val dataFrame = ss.read.option("header","true").csv("file:///C:\\Users\\wy\\Desktop\\spark-maven\\input\\students.csv")
    //dataFrame.show()
//    dataFrame.createTempView("students")
//    val head = ss.sql("select * from students")
//      .head()
//    println(head)
//
//
//    val head5 = ss.sql("select * from students")
//      .head(5)
//    println(head5)
//    ss.sql("select * from students")
//      .take(5)
//      .foreach(println(_))
//    println("__________")
//    val list5 = ss.sql("select * from students")
//      .takeAsList(5)
//    println(list5)
//    dataFrame.filter(" clazz='RB171' and score>90 ")
//      .show()
//    println("——————————————————")
//    ss.sql("select * from students where clazz='RB171' and score>90")
//      .show()
    //dataFrame.select().show()
    //ss.sql("select name as a, score+10, birth from students").drop("a").show()
//    dataFrame.orderBy(- dataFrame("score")).show()
//    dataFrame.orderBy(- dataFrame("id")).show()
//    val data1 = ss.read.option("header","true").csv("file:///C:\\Users\\wy\\Desktop\\spark-maven\\input\\students3.csv")
//    val data2 = ss.read.option("header","true").csv("file:///C:\\Users\\wy\\Desktop\\spark-maven\\input\\students4.csv")
//    data1.createTempView("students1")
//    data2.createTempView("students2")
//    data1.join(data2,Seq("name"),"inner").show()
//    data1.join(data2,Seq("name"),"outer").show()
//    data1.join(data2,data1("name")===data2("xm"),"inner").show()
//    val data = ss.read.option("header","true").csv("file:///C:\\Users\\wy\\Desktop\\spark-maven\\input\\students.csv")
//    data.createTempView("students")
//    case class Person(name: String, age: Long)
//    import ss.implicits._
//    Seq(Person("zhangsan", 17),Person("lisi", 20)).toDS().show()
    //data.groupBy("clazz").pivot("sex").count().show()
//    data.stat.freqItems(Seq("sex")).show()
//    data.stat.freqItems(Seq("name")).foreach(x=>print(x.toString()+","))
//    data.stat.freqItems(Seq("clazz")).foreach(x=>print(x.toString()+","))
//    data.withColumnRenamed("name","xxx").show()
//    data.withColumn("xm",data("name")).show()
//    val a = 1 to 20
//    val b = 'a' to 'z'
//    val data =ss.sparkContext.makeRDD(a.zip(b))
//    data.foreach(x=>print(x.toString()+","))
//    import ss.implicits._
//    data.map(x=>(x._1,x._2.toString))
//      .toDF("x","y").show()
//    ss.udf.register("copys",(x:String)=>x+x)
//    ss.sql("select copys(name),id  from students ")
//      .show()
//    data.write.format("parquet").mode("append").save("file:///C:\\Users\\wy\\Desktop\\spark-maven\\input")
    val datap = ss.read.load("file:///C:\\Users\\wy\\Desktop\\spark-maven\\input\\students.parquet")
    datap.show()
  }
}

object sql3{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val localPath = new File("").getAbsolutePath
    System.setProperty("hadoop.home.dir", localPath + "/hadoop-2.6.5")
    val ss = SparkSession
      .builder()
      .appName("sql1")
      .master("local[*]").getOrCreate()

//    val df = ss.read.format("jdbc")
//      .option("url", "jdbc:mysql://192.168.17.150:3306/spark?useSSL=false")
//      .option("dbtable", "students")
//      .option("user", "zzti")
//      .option("password", "123456")
//      .load()
//    df.show()
    val dataFrame = ss.read.option("header","true").csv("file:///C:\\Users\\wy\\Desktop\\spark-maven\\input\\students.csv")
    dataFrame.createTempView("stu")
    ss.sql("select name,id from stu")
      .show()
//    .write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://192.168.17.150:3306/spark?useSSL=false")
//      .option("dbtable", "students")
//      .option("user", "zzti")
//      .option("password", "123456")
//      .mode("append")
//      .save()
  }
}