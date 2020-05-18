package zutsoft.exam
object job6 {
  def main(args: Array[String]) {
    val source = scala.io.Source.fromFile("input/students.data", "UTF-8")
    .getLines().toArray
    val phone = source.map(_.trim().split(" "))
      .filter(_.length == 8)
      .map(_(7).toInt).sorted.foreach(println(_))

  }
}