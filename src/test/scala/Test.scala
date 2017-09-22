import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

/**
  * Created by cai on 9/18/17.
  */
object Test{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val a = Array(("a",1,"qa"), ("b",2,"qa"), ("c",8,"qa"), ("d",3,"qa"))
    val b = sc.parallelize(a)

    val maxKey2 = b.max()(new Ordering[Tuple3[String, Int, String]]() {
      override def compare(x: (String, Int, String), y: (String, Int, String)): Int =
        Ordering[Int].compare(x._2, y._2)
    })

    println(maxKey2)
  }
}
