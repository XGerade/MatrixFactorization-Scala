package eu.stratosphere.dima.recommendationsystem

import eu.stratosphere.api.scala.functions._
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;

class UserRatingVectorReducer extends GroupReduceFunction[(Int, PactVector), (Int, PactVector)] {
  override def apply (in : Iterator[(Int, PactVector)]) : (Int, PactVector) = {
    val result : PactVector = new PactVector()
    
    val first = in.next()
    
    val sum :Vector = first._2.get
    val userID = first._1;
    
    while (in.hasNext) {
      val temp = in.next()
      sum.assign(temp._2.get, Functions.PLUS)
    }
    result.set(new SequentialAccessSparseVector(sum))
    
    (userID, result)
  }
}