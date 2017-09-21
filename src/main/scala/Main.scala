import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{ Level, Logger }

/**
  * Created by cai on 9/18/17.
  */

object Main{

  def connectionBuilder: SparkContext ={
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    return sc
  }

  def runMain: Unit ={
    val sc = connectionBuilder
    var dataOperator = new DataOperator()
    val wordNeighborPairs = dataOperator.getWordNeighborPairs(sc, 3).map(x =>dataOperator.wordEntropy(x))
          .cache()

    wordNeighborPairs.collect().foreach(x => dataOperator.getTFByWord(x._1, wordNeighborPairs))
//    println(wordNeighborPairs.lookup("中国酒店"))
//    println(wordNeighborPairs.lookup("薇薇"))
//    val x = wordNeighborPairs.lookup("薇薇").toList(0)
//    println(x.toList.length)
//    for(a <- x.mapValues(iter => iter.map(_._2).toArray))
//      println(a)
//    dataReader.read(sc, 5).collect().foreach(println)
//    val result = dataReader.length(3)
//    for(x <- result){
//      println(x)
//    }

  }

  def main(args: Array[String]): Unit = {
    runMain
  }
}
