package zutsoft.start
// 出生年月最大值最小值
object job2 {
  def main(args: Array[String]) {
    val source = scala.io.Source.fromFile("input/students.data", "UTF-8").getLines().toArray
    val dates = source.map(_.trim().split("\t"))
    .filter(_.length == 8)
    .map(_(4).replaceAll("-", "").toInt)
    println("最大值：" + dates.max)
    println("最小值：" + dates.min)
  }
}