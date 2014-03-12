/*
 * Project: MatrixFactorization
 * @author Xugang Zhou
 * @author Fangzhou Yang
 * @version 1.0
 */

package eu.stratosphere.dima.recommendationsystem

import eu.stratosphere.api.scala.functions._
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.als.AlternatingLeastSquaresSolver;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import java.util.List;


import com.google.common.collect.Lists;

/*
 * This Cross class cross the item-rating-vector and the user-feature-matrix to produce a feature-vector for each item
 */
class ItemFeatureMatrixCrosser extends CrossFunction[(Int, PactVector), (Int, PactMatrix), (Int, PactVector, Int)] {
  /*
   * This override method defines how the feature-vector is calculated
   * It will find all the feature-vectors in the user-feature-matrix whose corresponding userID gets nonZero in the item-rating-vector
   * And calculate the feature-vector using ALS method 
   * @param l:(itemID, rating-vector)
   * @param r:(numUsers, user-feature-matrix) There would be only one user-feature-matrix in this case
   * @return (0, item-feature-vector, itemID) In the following step, all the feature-vector would be reduced to a feature-matrix
   * So the first field of the output would be 0 for the reduce operation's convenience
   */
  override def apply(l: (Int, PactVector), r: (Int, PactMatrix)): (Int, PactVector, Int) = {
    /*
     * Get item information and item-rating-vector
     */
    val itemID = l._1
    val itemVector : Vector = l._2.get
    /*
     * Get user-feature-matrix
     */
    val numUsers = r._1
    val userMatrix : Matrix = r._2.get
    /*
     * Extract all the user-feature-vectors from the user-feature-matrix
     * Add them to a HashMap
     */
    val userFeatureMatrix : OpenIntObjectHashMap[Vector] = new OpenIntObjectHashMap[Vector](numUsers)
    for (i <- 1 to Util.maxUserID) {
      val userVector : Vector  = userMatrix.viewRow(i)
      if (userVector.getNumNondefaultElements() > 0) {
        userFeatureMatrix.put(i, userVector)
      }
    }
    /*
     * Add all the feature-vectors whose corresponding userID gets nonZero in the item-rating-vector to a list
     */
    val userFeatureVectors : List[Vector] = Lists.newArrayListWithCapacity(itemVector.getNumNondefaultElements())
    var temp = itemVector.nonZeroes().iterator()
    while (temp.hasNext()) {
      val e = temp.next()
      val index = e.index()
       if(userFeatureMatrix.containsKey(index)) {
         userFeatureVectors.add(userFeatureMatrix.get(index));
       }
    }
    /*
     * Calculate the feature-vector for the item using ALS
     */
    val itemFeatureVector : Vector = AlternatingLeastSquaresSolver.solve(userFeatureVectors, itemVector, Util.lambda, Util.numFeatures)
    val itemFeatureVectorWritable : PactVector = new PactVector()
    itemFeatureVectorWritable.set(itemFeatureVector)
    (0, itemFeatureVectorWritable, itemID)
  }
}