package zutsoft.exam
// 出生年月最大值最小值
object job2 {
  def main(args: Array[String]) {
    val source = scala.io.Source.fromFile("input/students.data", "UTF-8").getLines().toArray
    val dates = source.map(_.trim().split("\t"))
    .filter(_.length == 8)
    .map(_(2).replaceAll("RB", "").toInt)
    println(dates.max+","+dates.min)
    println("最大值：" + dates.max)
    println("最小值：" + dates.min)
  }
}