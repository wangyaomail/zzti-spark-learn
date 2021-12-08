package zutsoft.exam
//（均值）求平均年龄
object job4 {
  def main(args: Array[String]) {
    val source = scala.io.Source.fromFile("input/students.data", "UTF-8")
    .getLines().toArray
    val a = source.map(_.trim().split("\t"))
      .filter(_.length == 8)
      .map(_(2).substring(2, 5).toFloat)
    println(a.reduce(_ + _) / a.length)
  }
}
