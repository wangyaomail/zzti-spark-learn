package zutsoft.start
//（均值）求平均年龄
object job4 {
  def main(args: Array[String]) {
    val source = scala.io.Source.fromFile("input/students.data", "UTF-8").getLines().toArray
    val age = source.map(_.trim().split(" "))
      .filter(_.length == 8)
      .map(2019-_(4).substring(0, 4).toInt)
    val sum = age.reduce(_ + _)
    val average = sum / age.length
    println("平均年龄为：" + average)
  }
}