import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

/**
  * Created by cai on 9/18/17.
  */
class DataReader extends Serializable{
  val DATAPATH = "/home/cai/Desktop/cai/data/corpus/answer_dev.txt"

  /*
    用递归获取一个字符串的子字符串：
      input：ABC
      output：[(ABC,l,r) (AB,l,r) (A,l,r)]
   */
  private def getSubStrings(element: String, left: String, veryRight: String): List[Tuple3[String, String, String]] ={
    val listBuffer = ListBuffer[Tuple3[String, String, String]]()

    def subStrings(element: String, right: String): Unit = {
      val elementLength = element.length
      if (elementLength > 1) {
        listBuffer.append((element, left, right))
        subStrings(element.substring(0, elementLength - 1), element.substring(elementLength - 1))
      } else {
        listBuffer.append((element, left, right))
      }
    }
    subStrings(element, veryRight)
    listBuffer.toList
  }

  /*
    调用则函数的前提是参数element小于等于NGram的window
    用递归获取一个字符串的子字符串：
      input：ABC, l, r
      output：[(A,l,r) (B,l,r) (C,l,r) (AB,l,r) (BC,l,r) (ABC,l,r)] (顺序不重要)
   */
  private def getNeighbors(element: String, left: String, right: String): List[Tuple3[String, String, String]] ={
    val listBuffer = ListBuffer[Tuple3[String, String, String]]()

    def subStrings(element: String, iter: Int): Unit = {
      var localLeft = left
      var localRight = right
      if(iter > 0){
        for(i <- 0 to element.length - iter) {
          if(i > 0)
            localLeft = element(i - 1).toString
          if(i + 1 < element.length)
            localRight = element(i + 1).toString
          listBuffer.append((element.substring(i, i + iter), localLeft, localRight))
        }

        subStrings(element, iter-1)
      }
    }

    subStrings(element, element.length)
    listBuffer.toList
  }


  /*
    根据Ngram中N的大小取出
   */
  def getNGram(sentence: String, maxGram: Int): List[Tuple3[String, String, String]] ={
    var allWords = List[Tuple3[String, String, String]]()
    assert(sentence.length > 0)
    assert(maxGram > 1)

    var left = ""
    var right = ""
    for(i <- 0 to sentence.length - maxGram){
      if(i > 0)
        left = sentence(i-1).toString
      if(i + maxGram + 1 < sentence.length)
        right = sentence(i + 1).toString
      allWords = allWords ::: getSubStrings(sentence.substring(i, i + maxGram), left, right)
    }
    // 解决余数
    if(sentence.length - maxGram < 0)
      allWords = allWords ::: getNeighbors(sentence, "", "")
    else if(sentence.length - maxGram >= 0)
      allWords = allWords ::: getNeighbors(
          sentence.substring(sentence.length - maxGram + 1, sentence.length),
          sentence(sentence.length - maxGram).toString, "")
    allWords
  }

  def read(sc: SparkContext, maxGram: Int): RDD[(String, Iterable[(String, String, String)])] ={
    val line = sc.textFile(DATAPATH)
    // 1. 把停用词，标点用空格替换。
    // 2. 把原来的句子根据空格分开，这样后面取“左右词”的时候就不会被空格影响。
    // 3. 去掉为空的句子
    // 4. 调用方法取Ngram的词
    line.map { x =>
      val x_filter = x.replaceAll("[" + UtilsTools.STOPWORDS + "]", " ").replaceAll("\\p{Punct}", " ").replaceAll("\\pP", " ")
        .replaceAll("　", " ").replaceAll("\\p{Blank}", " ").replaceAll("\\p{Space}", " ").replaceAll("\\p{Cntrl}", " ")
      x_filter
    }.flatMap(x => x.split(" ")).filter(x => x.length>0).flatMap(x => getNGram(x, maxGram)).keyBy(_._1).groupByKey

  }

  def length(maxGram: Int): List[Tuple3[String, String, String]] ={
    getNGram("你好吗", maxGram)
//    getNeighbors("你好吗")
//    getNeighbors("你好吗","嗨","?")
  }


}
