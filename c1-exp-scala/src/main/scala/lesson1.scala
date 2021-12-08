import scala.io.Source

object Test1 {
  def main(args: Array[String]): Unit = {
    val source = Source
      .fromFile("input/i_have_a_dream.data", "UTF-8")
      .getLines()
      .toArray
    source
      .flatMap(x => x.trim().split(" "))
      .map(x => (x, 1))
      .groupBy(x => x._1)
      .map(x => (x._1, x._2.length))
      .toList
      .sortBy(x => -x._2)
      .foreach(x => println(x))

    source
      .flatMap(_.trim().split(" "))
      .map((_, 1))
      .groupBy(_._1)
      .map(x => (x._1, x._2.length))
      .toList
      .sortBy(-_._2)
      .foreach(println(_))

  }
}

object Test2{
  def main(args: Array[String]): Unit = {
    val source = Source
      .fromFile("input/students.data", "UTF-8")
      .getLines().toArray

    println("任务一：（统计）统计男生和女生的总人数")
    source.map(_.trim().split("\t"))
      .filter(_.length == 8)
      .map(x => (x(3), 1))
      .groupBy(x => x._1)
      .mapValues(_.map(_._2).sum)
      .foreach(println(_))

    println("任务二：（最大值最小值）求出生年月日最大值")
    val dates = source
      .map(_.trim().split("\t"))
      .filter(_.length == 8)
      .map(_(4).replaceAll("-", "").toInt)
    println("最大值：" + dates.max)
    println("最小值：" + dates.min)

    println("任务三：（去重）求学生中出现的所有姓氏")
    val firstname = source.map(_.trim().split("\t"))
      .filter(_.length == 8)
      .map(_(0).substring(0, 1))
      .distinct.foreach(x=>print(x+","))

    println("任务四：（均值）求平均年龄")
    val age = source.map(_.trim().split("\t"))
      .filter(_.length == 8)
      .map(2019-_(4).substring(0, 4).toInt)
    val sum = age.reduce(_ + _)
    val average = sum / age.length
    println("平均年龄为：" + average)

    println("任务五：（分布）求学生的年龄分布")
    val age2 = source.map(_.trim.split("\t"))
      .filter(_.length == 8)
      .map(_(4).replaceAll("-", "").substring(0, 4).toInt)
      .map(x => 2019 - x)
      .map(x => (x, 1))
      .groupBy(x => x._1)
      .mapValues(_.map(_._2).sum)
      .foreach(println(_))

    println("任务六：（排序）按照同学们电话号码的大小排序")
    val phone = source.map(_.trim().split("\t"))
      .filter(_.length == 8)
      .map(_(5).toLong)
      .sorted
      .foreach(println(_))

    println("任务七：（索引）索引出相同生日下同学的姓名链表")
    val name = source.map(_.trim().split("\t"))
      .filter(_.length == 8)
      .map(x => (x(0), x(4).substring(5, 10).replaceAll("-", "")))
      .toList
      .groupBy(_._2)
      .mapValues(_.map(_._1))
      .toList
      .foreach(println(_))

    println("任务八：（topk）求出生日最大的5个同学的名字")
    val name2 = source.map(_.trim().split("\t"))
      .filter(_.length == 8)
      .map(x => (x(0), x(4).substring(5, 10).replaceAll("-", "")))
      .sortBy(x => x._2).take(5)
      .foreach(println(_))

  }
}

object Test3{
  def main(args: Array[String]): Unit = {
  }
}