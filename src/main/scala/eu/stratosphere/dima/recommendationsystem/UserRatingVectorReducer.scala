/*
 * Project: MatrixFactorization
 * @author Xugang Zhou
 * @author Fangzhou Yang
 * @version 1.0
 */

package eu.stratosphere.dima.recommendationsystem

import eu.stratosphere.api.scala.functions._
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.function.Functions;

/*
 * This Reduce class reduces all user-feature-vectors to a single user-feature-vector
 * Because each item has only one rating from the user, reduce operation would be adding all vectors up
 */
class UserRatingVectorReducer extends GroupReduceFunction[(Int, PactVector), (Int, PactVector)] {

  /*
   * This override method define how the reduce function works
   * @param in:Iterator(userID, rating-vector) rating-vectors of the userID
   * @return (userID, rating-vector) The user-rating-vector which contains ratings of all items from this user   
   */
  override def apply (in : Iterator[(Int, PactVector)]) : (Int, PactVector) = {

    val result : PactVector = new PactVector()
    /*
     * Get the user information from the first element of the list
     */
    val first = in.next()
    val sum :Vector = first._2.get
    val userID = first._1;
    /*
     * Iterate throw all the vector and add them up
     */
    while (in.hasNext) {
      val temp = in.next()
      sum.assign(temp._2.get, Functions.PLUS)
    }
    
    result.set(new SequentialAccessSparseVector(sum))    
    (userID, result)
  }
}