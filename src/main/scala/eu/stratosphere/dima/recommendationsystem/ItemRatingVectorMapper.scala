package eu.stratosphere.dima.recommendationsystem

import eu.stratosphere.api.scala.functions._
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.RandomAccessSparseVector;

class ItemRatingVectorMapper extends MapFunction[(Int, Int, Float), (Int, PactVector)] {
  override def apply(in: (Int, Int, Float)) : (Int, PactVector) = {

    val ratings : Vector = new RandomAccessSparseVector(Util.numUsers + 1, 1)
    val ratingVector : PactVector = new PactVector()
    ratingVector.reset(true)
    
    ratings.setQuick(in._1, in._3)
    
    ratingVector.set(ratings)
    
    (in._2, ratingVector)
    
  }
}