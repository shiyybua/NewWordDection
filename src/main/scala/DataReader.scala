import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by cai on 9/18/17.
  */
class DataReader extends Serializable{
  val DATAPATH = "/home/cai/Desktop/cai/data/corpus/answer_dev.txt"

  /*
    用递归获取一个字符串的子字符串：
      input：ABC
      output：ABC AB A
   */
  private def getSubStrings(element: String): List[String] ={
    val listBuffer = scala.collection.mutable.ListBuffer[String]()

    def subStrings(element: String): Unit = {
      val elementLength = element.length
      if (elementLength > 1) {
        listBuffer.append(element)
        subStrings(element.substring(0, elementLength - 1))
      } else {
        listBuffer.append(element)
      }
    }
    subStrings(element)
    listBuffer.toList
  }

  /*
    调用则函数的前提是参数element小于等于NGram的window
    用递归获取一个字符串的子字符串：
      input：ABC
      output：A B C AB BC ABC (顺序不重要)
   */
  private def getNeighbors(element: String): List[String] ={
    val listBuffer = scala.collection.mutable.ListBuffer[String]()

    def subStrings(element: String, iter: Int): Unit = {

      if(iter > 0){
        for(i <- 0 to element.length - iter)
          listBuffer.append(element.substring(i,i+iter))
        subStrings(element, iter-1)
      }
    }

    subStrings(element, element.length)
    listBuffer.toList
  }

  /*
    根据Ngram中N的大小取出
   */
  def getNGram(sentence: String, maxGram: Int): List[String] ={
    var allWords = List[String]()
    for(i <- 0 to sentence.length - maxGram){
      allWords = allWords ::: getSubStrings(sentence.substring(i, i + maxGram))
    }
    // 解决余数
    if(sentence.length - maxGram < 0)
      allWords = allWords ::: getNeighbors(sentence)
    else if(sentence.length - maxGram >= 0)
      allWords = allWords ::: getNeighbors(sentence.substring(sentence.length - maxGram, sentence.length))

    allWords
  }

  def read(sc: SparkContext, maxGram: Int): RDD[(String, Int)] ={
    val line = sc.textFile(DATAPATH)
    line.flatMap(x => getSubStrings(x)).map((_, 1)).reduceByKey(_ + _)
  }

  def length(maxGram: Int): List[String] ={
    getNGram("你好吗我很好", maxGram)
//    getNeighbors("你好吗")
  }


}
