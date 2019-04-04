package zutsoft.start

import scala.io.Source

object WordCount {
  def main(args: Array[String]) {
    val source = Source.fromFile("input/test.data", "UTF-8").getLines().toArray
    source.flatMap(x => x.trim().split(" ")).map(x => (x, 1)).groupBy(x => x._1).map(x => (x._1, x._2.length)).toList.sortBy(x => -x._2).foreach(x => println(x))
  }

}
