import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cai on 9/18/17.
  */

object Main{

  def connectionBuilder: SparkContext ={
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    return sc
  }

  def runMain: Unit ={
//    val sc = connectionBuilder
    var dataReader = new DataReader()
//    dataReader.read(sc, 6).collect().foreach(println)

    for(x <- dataReader.length(6))
      println(x)
  }

  def main(args: Array[String]): Unit = {
    runMain
  }
}
