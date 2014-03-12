/*
 * Project: MatrixFactorization
 * @author Xugang Zhou
 * @author Fangzhou Yang
 * @version 1.0
 */

package eu.stratosphere.dima.recommendationsystem;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.als.AlternatingLeastSquaresSolver;
import org.apache.mahout.math.map.OpenIntObjectHashMap;

import eu.stratosphere.api.scala.functions._

/*
 * This Cross class cross all the user-feature-vector and all the item-feature-vector
 * to produce the rating for each (userID, itemID) pair
 */
class PredicetionCrosser extends CrossFunction[(Int, PactVector, Int), (Int, PactVector, Int), (Int, Int, Float)]{
  /*
   * The override method defines how the prediction calculated
   * @param l:(0, item-feature-Vector, itemID)
   * @param r:(0, user-feature-Vector, userID)
   * @return (userID, itemID, prediction-rating)
   */
  override def apply (l: (Int, PactVector, Int), r: (Int, PactVector, Int)) : (Int, Int, Float) = {
    val userID = r._3
    val itemID = l._3
    val itemVector = l._2.get
    val userVector = r._2.get
    /*
     * Calculate the prediction by dot-multiply two vectors
     */
    val predictRating : Float = itemVector.dot(userVector).toFloat
    (userID, itemID, predictRating)
  }
}
