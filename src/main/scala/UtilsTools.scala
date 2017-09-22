/**
  * Created by cai on 9/20/17.
  */
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
}