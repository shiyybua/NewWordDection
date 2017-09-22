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
    val MAX_GRAM = 4
    val sc = connectionBuilder
    var dataOperator = new DataOperator()
    // ＴＯＤＯ：max gram要比前面多２个
    val wordNeighborPairs = dataOperator.getWordNeighborPairs(sc, MAX_GRAM).map(x =>dataOperator.wordEntropy(x)).cache()
    val wordNeighborPairsCollection = wordNeighborPairs.collect
    val lookup = UtilsTools.convertKeyArray2MapTF(wordNeighborPairsCollection)
//    wordNeighborPairs.collect().foreach(println)
    val wordInfo = wordNeighborPairs.filter(x => x._1.length < MAX_GRAM).flatMap(x => dataOperator.getTFByWord(x, lookup)).cache()
    val PMIMax = UtilsTools.getMax(wordInfo,2)
    val PMIMin = UtilsTools.getMin(wordInfo,2)

    val leftEntropyMax = UtilsTools.getMax(wordInfo,3)
    val leftEntropyMin = UtilsTools.getMin(wordInfo,3)

    val rightEntropyMax = UtilsTools.getMax(wordInfo,4)
    val rightEntropyMin = UtilsTools.getMin(wordInfo,4)

    wordInfo.map(x => dataOperator.normalization(x, PMIMax, PMIMin,
      leftEntropyMax, leftEntropyMin, rightEntropyMax, rightEntropyMin))
      .sortBy(x => x._2).collect().foreach(println)
  }

  def main(args: Array[String]): Unit = {
    runMain
  }
}
