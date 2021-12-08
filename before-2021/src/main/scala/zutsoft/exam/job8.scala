package zutsoft.exam
object job8 {
  def main(args: Array[String]) {
    val source = scala.io.Source.fromFile("input/students.data", "UTF-8")
            .getLines().toArray
    val phone = source.map(_.trim().split(" "))
      .filter(_.length == 8)
      .map(x => (x(0), x(4).substring(8, 10)))
      .sortBy(x => x._2).take(3)
      .foreach(println(_))

  }
}