package zutsoft.exam
//（去重）求学生中出现的所有姓氏
object job3 {
  def main(args: Array[String]) {
    val source = scala.io.Source.fromFile("input/students.data", "UTF-8")
      .getLines().toArray
    val firstname = source.map(_.trim().split("\t"))
      .filter(_.length == 8)
      .map(_(0).substring(1, 2))
      .distinct.foreach(print(_))
  }
}