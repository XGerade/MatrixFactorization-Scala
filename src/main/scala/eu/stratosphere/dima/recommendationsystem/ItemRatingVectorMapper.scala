/*
 * Project: MatrixFactorization
 * @author Xugang Zhou
 * @author Fangzhou Yang
 * @version 1.0
 */

package eu.stratosphere.dima.recommendationsystem

import eu.stratosphere.api.scala.functions._
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.RandomAccessSparseVector;

/*
 * This Map class maps each user-item-rating to a item-feature-vector 
 * which contains only information of the user's rating
 */
class ItemRatingVectorMapper extends MapFunction[(Int, Int, Float), (Int, PactVector)] {

  /*
   * This override method define how the map function works
   * @param in:(userID, itemID, rating) A rating entry
   * @return (itemID, item-feature-vector) The item-rating-vector which contains only rating information of that user  
  */
  override def apply(in: (Int, Int, Float)) : (Int, PactVector) = {

    /*
     * The userID starts from 1
     * So the initialized cardinality would be set to numUsers + 1
     */
    val ratings : Vector = new RandomAccessSparseVector(Util.numUsers + 1, 1)
    val ratingVector : PactVector = new PactVector()
    /*
     * Set Vector to use float which double is not supported by stratosphere
     */
    ratingVector.reset(true)
    ratings.setQuick(in._1, in._3)    
    ratingVector.set(ratings)
    
    (in._2, ratingVector)
    
  }
}