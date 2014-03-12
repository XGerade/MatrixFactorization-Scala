/*
 * Project: MatrixFactorization
 * @author Xugang Zhou
 * @author Fangzhou Yang
 * @version 1.0
 */

/*
 * This Cross class cross the user-rating-vector and the item-feature-matrix to produce a feature-vector for each user
 */
package eu.stratosphere.dima.recommendationsystem

import eu.stratosphere.api.scala.functions._
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.als.AlternatingLeastSquaresSolver;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import java.util.List;


import com.google.common.collect.Lists;

class UserFeatureMatrixCrosser extends CrossFunction[(Int, PactVector), (Int, PactMatrix), (Int, PactVector, Int)] {
  /*
   * This override method defines how the feature-vector is calculated
   * It will find all the feature-vectors in the item-feature-matrix whose corresponding itemID gets nonZero in the user-rating-vector
   * And calculate the feature-vector using ALS method 
   * @param l:(userID, rating-vector)
   * @param r:(numItems, item-feature-matrix) There would be only one item-feature-matrix in this case
   * @return (0, user-feature-vector, userID) In the following step, all the feature-vector would be reduced to a feature-matrix
   * So the first field of the output would be 0 for the reduce operation's convenience
   */
  override def apply(l: (Int, PactVector), r: (Int, PactMatrix)): (Int, PactVector, Int) = {
    /*
     * Get user information and user-rating-vector
     */
    val userID = l._1
    val userVector : Vector = l._2.get
    /*
     * Get item-feature-matrix
     */
    val numItems = r._1
    val itemMatrix : Matrix = r._2.get
     /*
     * Extract all the item-feature-vectors from the item-feature-matrix
     * Add them to a HashMap
     */
   val itemFeatureMatrix : OpenIntObjectHashMap[Vector] = new OpenIntObjectHashMap[Vector](numItems)
    for (i <- 1 to Util.maxItemID) {
      val itemVector : Vector  = itemMatrix.viewRow(i)
      if (itemVector.getNumNondefaultElements() > 0) {
        itemFeatureMatrix.put(i, itemVector)
      }
    }
    /*
     * Add all the feature-vectors whose corresponding itemID gets nonZero in the user-rating-vector to a list
     */
    val itemFeatureVectors : List[Vector] = Lists.newArrayListWithCapacity(userVector.getNumNondefaultElements())
    var temp = userVector.nonZeroes().iterator()
    while (temp.hasNext()) {
      val e = temp.next()
      val index = e.index()
       if(itemFeatureMatrix.containsKey(index)) {
         itemFeatureVectors.add(itemFeatureMatrix.get(index));
       }
    }
    /*
     * Calculate the feature-vector for the user using ALS
     */
    val userFeatureVector : Vector = AlternatingLeastSquaresSolver.solve(itemFeatureVectors, userVector, Util.lambda, Util.numFeatures)
    val userFeatureVectorWritable : PactVector = new PactVector()
    userFeatureVectorWritable.set(userFeatureVector)
    (0, userFeatureVectorWritable, userID)
  }
}