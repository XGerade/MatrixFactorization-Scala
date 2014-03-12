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
 * This Reduce class reduces all item-feature-vectors to a single item-feature-vector
 * Because each user has only one rating to the item, reduce operation would be adding all vectors up
 */
class ItemRatingVectorReducer extends GroupReduceFunction[(Int, PactVector), (Int, PactVector)] {

  /*
   * This override method define how the reduce function works
   * @param in:Iterator(itemID, rating-vector) rating-vectors of the itemID
   * @return (itemID, rating-vector) The item-rating-vector which contains all users' ratings of this item   
   */
  override def apply (in : Iterator[(Int, PactVector)]) : (Int, PactVector) = {

    val result : PactVector = new PactVector()
    /*
     * Get the item information from the first element of the list
     */
    val first = in.next()    
    val sum :Vector = first._2.get
    val itemID = first._1;
    /*
     * Iterate throw all the vector and add them up
     */
    while (in.hasNext) {
      val temp = in.next()
      sum.assign(temp._2.get, Functions.PLUS)
    }
    
    result.set(new SequentialAccessSparseVector(sum))
    (itemID, result)
  }
}