import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

/**
  * Created by cai on 9/18/17.
  */
object Test{
  def main(args: Array[String]): Unit = {
    val treasureMap = Map[Int, String]()
    treasureMap += (1 -> "test")
    treasureMap(2) = "666"
    println(treasureMap.contains(2))
    println(treasureMap(2))

  }
}
