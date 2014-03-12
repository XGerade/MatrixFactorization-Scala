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
import org.apache.mahout.math.DenseMatrix;

/*
 * This Reduce class reduce all the user-feature-vector to a single user-feature-matrix
 */
class UserFeatureMatrixReducer extends GroupReduceFunction[(Int, PactVector, Int), (Int, PactMatrix)] {
  /*
   * This override Method defines how the reduce operations works
   * @param in:Iterator[(0, user-feature-vector, userID)] Here are all user-feature-vector with its ID
   * @return (numUsers, user-feature-matrix)
   */
  override def apply(in: Iterator[(Int, PactVector, Int)]) : (Int, PactMatrix) = {
    /*
     * The userID starts from 1
     * So the initialized cardinality would be set to numUsers + 1
     */
    val features : Matrix = new DenseMatrix(Util.numUsers + 1, Util.numFeatures)
    /*
     * Add all user-feature-vector to the matrix with index userID
     */
    while (in.hasNext) {
      val temp = in.next()
      val userID = temp._3
      val userFeature = temp._2.get
      features.assignRow(userID, userFeature)
    }
    
    val featureMatrix : PactMatrix = new PactMatrix()
    featureMatrix.set(features)
    (Util.numUsers, featureMatrix)
  }
}