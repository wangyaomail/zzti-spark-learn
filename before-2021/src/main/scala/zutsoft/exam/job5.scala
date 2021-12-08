package zutsoft.exam
//（分布）求学生的年龄分布
object job5 {
  def main(args: Array[String]) {
    val source = scala.io.Source.fromFile("input/students.data", "UTF-8").getLines().toArray
val age = source.map(_.trim.split("\t"))
  .filter(_.length == 8)
  .map(_(2))
  .map(x => (x, 1))
  .groupBy(x => x._1)
  .mapValues(_.map(_._2).sum)
  .foreach(print(_))

  }
}