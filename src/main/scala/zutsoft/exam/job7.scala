package zutsoft.exam
object job7 {
  def main(args: Array[String]) {
    val source = scala.io.Source.fromFile("input/students.data", "UTF-8")
                  .getLines().toArray
    val phone = source.map(_.trim().split(" "))
      .filter(_.length == 8)
      .map(x => (x(0), x(4).substring(5, 10).replaceAll("-", "")))
      .toList
      .groupBy(_._2)
      .mapValues(_.map(_._1))
      .toList
      .foreach(println(_))
  }
}


