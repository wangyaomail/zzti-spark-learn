import com.alibaba.fastjson.JSON

import scala.io.Source
import scala.util.matching.Regex


//张祥德	RB17101	RB171	男	1997-02-10	11122223333	河南省郑州市1号	88
//冯成刚	RB17102	RB171	女	1996-10-01	18837110115	河南省洛阳市2号	86
//卢伟兴	RB17103	RB171	男	1998-08-02	19999228822	河南省开封市3号	95
//杨飞龙	RB17104	RB171	男	1996-08-09	13322554455	河南省安阳市4号	91
//姜松林	RB17201	RB172	女	1997-01-03	13688552244	河南省鹤壁市1号	75
//高飞	RB17202	RB172	男	1996-08-27	13522114455	河南省新乡市2号	68
//何桦	RB17203	RB172	女	1997-12-20	13566998855	河南省焦作市3号	84
//高天阳	RB17204	RB172	男	1999-11-08	13688446622	河南省濮阳市4号	77
//周存富	RB17301	RB173	男	1996-05-28	13699552658	河南省许昌市1号	93
//罗鹏	RB17302	RB173	男	1998-03-02	13365298741	河南省漯河市2号	85
//宋立昌	RB17401	RB174	男	1995-05-28	13596325874	河南省南阳市3号	81
//杨国胜	RB17402	RB174	男	1996-03-02	13256987456	河南省信阳市4号	91
//徐子文	RB17403	RB174	男	1998-05-28	13523654789	河南省周口市5号	85
//马彦	RB17404	RB174	女	1997-03-02	13526845962	河南省郑州市6号	73


object Test3{
  def main(args: Array[String]): Unit = {
    val source = Source
      .fromFile("input/students.data")
      .getLines()
      .toArray
      .map(_.trim().split("\t"))
      .filter(_.length==8)
    // 任务3 去重
    source
      .map(_(0).substring(0,1))
      .distinct
      .foreach(println)
    // 任务4 均值
    source
      .map(_(4).substring(0,4).toInt)
      .map(2021-_)
      .sorted
      .foreach(println)
    // 任务5 分布
    source
      .map(_(4).substring(0,4).toInt)
      .map(2021-_)
      .groupBy(x=>x)   //(x ,  List(x,x,x,x) )
      .map(x=>(x._1, x._2.length))
      .toList
      .sortBy(_._1)
      .foreach(println)
    // 任务6 排序
    source
      .map(_(5).toLong)
      .sorted
      .foreach(println)
    // 任务7 姓名链表
    source
      .map(x=>(x(0),2021 - x(4).substring(0,4).toInt))
      .groupBy(_._2) // (age, List((name, age),(name, age),(name, age),(name, age),(name, age))
      .map(x=>(x._1, x._2.map(_._1).toList))
      .toList
      .sortBy(_._1)
      .foreach(println)
    // 任务8 topk
    source
      .map(x=>(x(0), x(4).substring(4,10).replaceAll("-","").toInt))
      .sortBy(_._2)  // (name, birth m-d)
      .map(x=>(x._2, x._1))
      .takeRight(5)
      .foreach(println)
  }
}

object Test4{
  def main(args: Array[String]): Unit = {
    // E:\data\nlp_chinese_corpus\webtext2019zh
    val source = Source
      .fromFile("E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_test.json")
      .getLines()
      .map(JSON.parseObject(_))
      .toArray

    // 测试数据是否维度齐全
    val dim_test = source
      .map(x=>List(x.containsKey("qid"),x.containsKey("title"),x.containsKey("desc"),x.containsKey("topic"),
        x.containsKey("star"),x.containsKey("content"),x.containsKey("answer_id"),x.containsKey("answerer_tags")))
      .map(_.map(if(_) 1 else 0))
      .reduce((a,b)=>List(a(0)+b(0),a(1)+b(1),a(2)+b(2),a(3)+b(3)
        ,a(4)+b(4),a(5)+b(5),a(6)+b(6),a(7)+b(7)))
    println(dim_test)
    // 封装
    class Answer(var qid:Int, var title:String, var desc:String, var topic:String,
                 var star:Int, var content:String, var answer_id:Int, var answerer_tags:String){
      override  def toString()={
        qid+":"+title+":"+answer_id+":"+answerer_tags
      }
    }
    val ansArr = source
      .map(x=>new Answer(x.getIntValue("qid"),x.getString("title"), x.getString("desc"), x.getString("topic"),
        x.getIntValue("star"),x.getString("content"), x.getIntValue("answer_id"),x.getString("answerer_tags")))
      .sortBy(_.qid)
      .toArray
    val dis_len = ansArr
      .map(_.answer_id)
      .distinct
      .length - ansArr.length
    println("差别长度",dis_len)
    val ansMap = ansArr
      .map(x=>(x.answer_id, x))
      .toMap
    val qMap = ansArr
      .map(x=>(x.qid, x.title))
      .groupBy(_._1)
      .map(x=>(x._1, x._2(0)))
      .toMap
    // 答案多少
    val ansNumArr = ansArr
      .map(_.qid)
      .groupBy(x=>x)
      .map(x=>(x._1,x._2.length))
      .toList
      .sortBy(_._2)
    println("答案最多的问题",ansNumArr.takeRight(3).map(x=>(qMap.get(x._1),x._2)))
    println("答案最少的问题",ansNumArr.take(3).map(x=>(qMap.get(x._1),x._2)))
    // 标签
    val pattern = new Regex("[\\u4E00-\\u9FA5]+")
    val tagArr = ansArr
      .map(_.answerer_tags)
      .filter(_.length>0)
      .flatMap(_.split(" ").toList)
      .filter(_.length>1)
      .filter(x=>pattern.findFirstIn(x).size>0)
      .groupBy(x=>x)
      .map(x=>(x._1,x._2.length))
      .toList
      .sortBy(_._2)
    println("最多的标签",tagArr.takeRight(3))
    println("最少的标签",tagArr.take(3))


    println("end code")
  }
}








