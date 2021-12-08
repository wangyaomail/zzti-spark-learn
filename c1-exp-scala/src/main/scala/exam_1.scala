import scala.io.Source

//张祥德,RB17101,RB171,男,1997-02-10,11122223333,河南省郑州市1号,88
//冯成刚,RB17102,RB171,女,1996-10-01,18837110115,河南省洛阳市2号,86
//卢伟兴,RB17103,RB171,男,1998-08-02,19999228822,河南省开封市3号,95
//杨飞龙,RB17104,RB171,男,1996-08-09,13322554455,河南省安阳市4号,91
//姜松林,RB17201,RB172,女,1997-01-03,13688552244,河南省鹤壁市1号,75
//高飞,RB17202,RB172,男,1996-08-27,13522114455,河南省新乡市2号,68
//何桦,RB17203,RB172,女,1997-12-20,13566998855,河南省焦作市3号,84
//高天阳,RB17204,RB172,男,1999-11-08,13688446622,河南省濮阳市4号,77
//周存富,RB17301,RB173,男,1996-05-28,13699552658,河南省许昌市1号,93
//罗鹏,RB17302,RB173,男,1998-03-02,13365298741,河南省漯河市2号,85
//宋立昌,RB17401,RB174,男,1995-05-28,13596325874,河南省南阳市3号,81
//杨国胜,RB17402,RB174,男,1996-03-02,13256987456,河南省信阳市4号,91
//徐子文,RB17403,RB174,男,1998-05-28,13523654789,河南省周口市5号,85
//马彦,RB17404,RB174,女,1997-03-02,13526845962,河南省郑州市6号,73

object exam_10_26{
  def main(args: Array[String]): Unit = {
    println("1：90分以上的学生名单")
    scala.io.Source
      .fromFile("input/students_exam_1.data", "UTF-8")
      .getLines()
      .map(_.trim().split(","))
      .filter(_.length == 8)
      .map(x => (x(0), x(7)))
      .filter(_._2.toInt>=90)
      .foreach(println(_))

    println("2：家住许昌的男生数量")
    val xc_boys = scala.io.Source
      .fromFile("input/students_exam_1.data", "UTF-8")
      .getLines()
      .map(_.trim().split(","))
      .filter(_.length == 8)
      .map(x => (x(3), x(6)))
      .filter(_._1.equals("男"))
      .filter(_._2.contains("许昌"))
    print(xc_boys.size)

    println("3：RB172班的平均分")
    val score172 = scala.io.Source
      .fromFile("input/students_exam_1.data", "UTF-8")
      .getLines()
      .map(_.trim().split(","))
      .filter(_.length == 8)
      .map(x => (x(2), x(7)))
      .filter(_._1.equals("RB172"))
      .map(_._2.toInt)
      .toList
    print(score172.sum/score172.size)

    println("4：成绩最高的三位学生姓名")
    scala.io.Source
      .fromFile("input/students_exam_1.data", "UTF-8")
      .getLines()
      .map(_.trim().split(","))
      .filter(_.length == 8)
      .map(x => (x(0), x(7).toInt))
      .toList
      .sortBy(_._2)
      .takeRight(3)
      .foreach(println(_))

    println("5：各班学生人数")
    scala.io.Source
      .fromFile("input/students_exam_1.data", "UTF-8")
      .getLines()
      .map(_.trim().split(","))
      .filter(_.length == 8)
      .map(x => (x(2), 1))
      .toList
      .groupBy(x => x._1)
      .mapValues(_.map(_._2).sum)
      .foreach(println(_))

  }
}