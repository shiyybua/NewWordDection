/**
  * Created by cai on 9/20/17.
  */
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Map

object UtilsTools{
  val STOPWORDS = "啊呢吗嘛哦呀"
  val PUNCTUATIONS = "。，！;：《》“”？,.!?;:"

  def convertKeyArray2MapTF(arr: Array[(String, Tuple5[Int, Double, Double, List[String], List[String]])]):
    Map[String, Tuple3[Int, Double, Double]] ={

    val lookup = Map[String, Tuple3[Int, Double, Double]]()
    for(x <- arr){
      lookup(x._1) = (x._2._1,x._2._2,x._2._3)
    }
    lookup
  }

  def getMax(arr: RDD[(String, Double, Double, Double)], index: Int): Double ={
    val maxKey2 = arr.max()(new Ordering[Tuple4[String, Double, Double, Double]]() {
      override def compare(x: (String, Double, Double, Double), y: (String, Double, Double, Double)): Int = {
        if(index == 2)
          Ordering[Double].compare(x._2, y._2)
        else if(index == 3)
          Ordering[Double].compare(x._3, y._3)
        else
          Ordering[Double].compare(x._4, y._4)
      }
    })
    if(index == 2)
      maxKey2._2
    else if(index == 3)
      maxKey2._3
    else
      maxKey2._4
  }

  def getMin(arr: RDD[(String, Double, Double, Double)], index: Int): Double ={
    val maxKey2 = arr.min()(new Ordering[Tuple4[String, Double, Double, Double]]() {
      override def compare(x: (String, Double, Double, Double), y: (String, Double, Double, Double)): Int = {
        if(index == 2)
          Ordering[Double].compare(x._2, y._2)
        else if(index == 3)
          Ordering[Double].compare(x._3, y._3)
        else
          Ordering[Double].compare(x._4, y._4)
      }
    })
    if(index == 2)
      maxKey2._2
    else if(index == 3)
      maxKey2._3
    else
      maxKey2._4
  }

}