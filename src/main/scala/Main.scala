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
//    val sc = connectionBuilder
    var dataReader = new DataReader()
//    dataReader.read(sc, 3).collect().foreach(println)
    val result = dataReader.length(3)
    for(x <- result){
      println(x)
    }

  }

  def main(args: Array[String]): Unit = {
    runMain
  }
}
