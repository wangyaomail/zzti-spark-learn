
import scala.io.Source
import scala.util.Random
import scala.util.matching.Regex

object PreTest {
  def main(args: Array[String]): Unit = {
    println(Random.nextInt(10))
    println(Random.nextInt(10))
    println(Random.nextInt(10))
    println(Random.nextInt(10))
    println(Random.nextInt(10))
  }
}

object Test1 {
  def main(args: Array[String]): Unit = {
    val source = Source
      .fromFile("E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_test.json", "UTF-8")
      .getLines()
      .toArray

    // print(source.length)

    //    source
    //      .map(JSON.parseObject)
    //      .take(3)
    //      .foreach(println)
    //
    //    source
    //      .filter(_=>Random.nextInt(source.length)>source.length*0.9)
    //      .map(JSON.parseObject)
    //      .take(3)
    //      .foreach(println)

    //    def hasKey(x:JSONObject, k:String):Int={
    //      if(x.containsKey(k)) 1 else 0
    //    }
    //    val full_dim=source
    //      .map(JSON.parseObject(_))
    //      .map(x=>(hasKey(x,"qid"),hasKey(x,"title"),hasKey(x,"desc"),hasKey(x,"topic"),hasKey(x,"star"),hasKey(x,"content"),hasKey(x,"answer_id"),hasKey(x,"answerer_tags")))
    //      .reduce((a,b)=>(a._1+b._1,a._2+b._2,a._3+b._3,a._4+b._4,a._5+b._5,a._6+b._6,a._7+b._7,a._8+b._8))
    //    println(full_dim)

    //{
    //    "qid": <qid>,
    //    "title": <title>,
    //    "desc": <desc>,
    //    "topic": <topic>,
    //    "star": <star>,
    //    "content": <content>,
    //    "answer_id": <answer_id>,
    //    "answerer_tags": <answerer_tags>
    //}
    //    class Answer(){
    //      var qid:Int
    //      var title:String
    //      var desc:String
    //      var topic:String
    //      var star:Int
    //      var content:String
    //      var answer_id:String
    //      var answer_tags:String
    //    }
    class Answer(var qid: Int, var title: String, var desc: String, var topic: String, var star: Int, var content: String, var answer_id: String, var answerer_tags: String) {
      override def toString()={
        qid+":"+title+":"+answer_id+":"+content
      }
    }
//    source
//      .map(JSON.parseObject(_))
//      .map(x=>new Answer(x.getIntValue("qid"),x.getString("title"),x.getString("desc"),x.getString("topic"),x.getIntValue("star"),x.getString("content"),x.getString("answer_id"),x.getString("answerer_tags")))
//      .sortBy(_.answer_id)
//      .take(3)
//      .foreach(println)

//    val dis = source
//      .map(JSON.parseObject(_))
//      .map(_.getIntValue("answer_id"))
//      .distinct.length - source.length
//    println(dis)

    val ansArr = source
      .map(JSON.parseObject(_))
      .map(x=>new Answer(x.getIntValue("qid"),x.getString("title"),x.getString("desc"),x.getString("topic"),x.getIntValue("star"),x.getString("content"),x.getString("answer_id"),x.getString("answerer_tags")))
      .sortBy(_.qid)
      .toArray
    val ansMap = ansArr
      .map(x=>(x.answer_id, x))
      .toMap
    val qMap = ansArr
      .groupBy(_.qid)
      .map(x=>(x._1,x._2.head.title))
      .toMap

//    val qByCount = ansArr
//      .groupBy(_.qid)
//      .map(x=>(x._1,x._2.length))
//      .toArray
//      .sortBy(_._2)
//    println("回答最少的", qMap.get(qByCount.head._1), qByCount.head._2)
//    println("回答最多的", qMap.get(qByCount.last._1), qByCount.last._2)
//    println("平均回答", ansArr.length.toFloat/qByCount.length)

    // 出现最多的标签
    val pattern = new Regex("[\\u4E00-\\u9FA5]+")
    val tagArr = ansArr
      .map(_.answerer_tags)
      .filter(x=>x!=null&&x.length>0)
      .flatMap(_.split(" ").toList)
      .map((_,1))
      .groupBy(_._1)
      .map(x=>(x._1,x._2.length))
      .toList
      .filter(x=>x._2>1) // 判断长度大于1
      .filter(x=> pattern.findFirstIn(x._1).size>0) // 只要汉字
      .sortBy(_._2)

    tagArr.takeRight(10).foreach(println)
    println("-----")
    // 高赞用户
    val mostStarUsers = ansArr
      .map(x=>(x.answerer_tags, x.star))
      .groupBy(_._1)
      .map(x=>(x._1,x._2.map(_._2).reduce(_+_)))
      .toList
      .sortBy(_._2)

    mostStarUsers.takeRight(10).foreach(println)
    println("-----")
    // 答案里的高频词，引入ansj分词器
    val answerToks = ansArr
      .map(_.content)
      .flatMap(BaseAnalysis.parse(_).getTerms().asScala.map(_.getName)) // java的list转为scala的list
      .groupBy(x=>x)
      .map(x=>(x._1,x._2.length))
      .toList
      .filter(x=>x._1.size>1) // 要求词不能只有一位
      .sortBy(_._2)

    answerToks.takeRight(10).foreach(println)
    println("-----")

    // 问题情感倾向
    val pos_dict = Source
      .fromFile("dict/正面词.dict", "UTF-8")
      .getLines()
      .toSet
    val neg_dict = Source
      .fromFile("dict/负面词.dict", "UTF-8")
      .getLines()
      .toSet
    println("正面词",pos_dict.take(10))
    println("负面词",neg_dict.take(10))
    val answerSentiment = ansArr
      .map(x=>(x.answer_id, x.content))
      .map(x=>(x._1, BaseAnalysis.parse(x._2).getTerms().asScala.map(_.getName)))
      .map(x=>(x._1, x._2.map(a=>{if(pos_dict.contains(a)) 1 else if(neg_dict.contains(a)) -1 else 0 })))
      .map(x=>(x._1, x._2.fold(0)(_+_)))
      .sortBy(_._2)

    println("最正面的答案",answerSentiment.takeRight(3).map(x=>ansMap.get(x._1)).toList)
    println("最负面的答案",answerSentiment.take(3).map(x=>ansMap.get(x._1)).toList)
    println("正面答案数量",answerSentiment.filter(_._2>0).length)
    println("负面答案数量",answerSentiment.filter(_._2<0).length)
    println("中立答案数量",answerSentiment.filter(_._2==0).length)


    //任务1：请统计问题标题的平均长度
    val titlelength = qMap
      .toList
      .map(_._1)
      .take(5)
      .foreach(println)


//      .map(x=>x.getString("title").length)
//      .toList
//      .reduce((a,b)=>((a+b)/2))//求平均长度
//    println(task1)
//
//    //任务2：请挖掘问题标题长度和回答数量之间的关系，问题长度多长的时候，能够得到最多的回答
//    val task2 = source
//      .map(x=>(x.getString("title").length,x.getString("answerer_tags").length))
//      .toList
//      .groupBy(_._1)//根据问题长度进行分组
//      .map(x=>(x._1,x._2
//        .map(_._2)
//        .reduce((a,b)=>(a+b)/2)))//求每个问题长度的回答的平均个数，即：（问题长度，答案的平均个数）
//      .toArray
//      .sortBy(_._2)//根据答案平均长度进行排序
//      .takeRight(1)//得到平均个数最多的问题长度
//      .foreach(println(_))
//    //任务3：请在topic字段下进行统计最热的10个topic
//    val task3 = source
//      .map(x=>(x.getString("topic"),1))
//      .groupBy(_._1)                //根据topic进行分组
//      .map(x=>(x._1,-1*x._2.length))//得到元组（topic，topic的数量的负数）
//      .toArray
//      .sortBy(_._2)                 //根据数量进行排序
//      .take(10)                     //取到第二的元素绝对值最大的前十个元组（topic，topic数量的负数）
//      .map(x=>(x._1,-1*x._2))       //将第二个元素进行去反变为数量（topic，topic数量）
//      .foreach(println)
//    //任务4：尝试手动构造敏感词词典，并筛选出20个敏感问题
//    val task4 = source
//      .map(x=>(x.getString("title")))
//      .filter(_.contains("药"))//筛选出问题中含有药的问题
//      .take(20)
//      .foreach(println(_))



    // 绘制标签云

    println("code end")
  }
}