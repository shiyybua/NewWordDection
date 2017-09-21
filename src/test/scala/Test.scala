import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

/**
  * Created by cai on 9/18/17.
  */
object Test{
  def main(args: Array[String]): Unit = {
    val a = 3
    val b = 10
    println(a*math.log(1.0*a/b))

  }
}
