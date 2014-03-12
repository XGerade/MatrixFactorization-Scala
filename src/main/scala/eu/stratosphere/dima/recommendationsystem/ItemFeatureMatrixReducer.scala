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
 * This Reduce class reduce all the item-feature-vector to a single item-feature-matrix
 */
class ItemFeatureMatrixReducer extends GroupReduceFunction[(Int, PactVector, Int), (Int, PactMatrix)] {  
  /*
   * This override Method defines how the reduce operations works
   * @param in:Iterator[(0, item-feature-vector, itemID)] Here are all item-feature-vector with its ID
   * @return (numItems, item-feature-matrix)
   */
  override def apply(in: Iterator[(Int, PactVector, Int)]) : (Int, PactMatrix) = {    
    /*
     * The itemID starts from 1
     * So the initialized cardinality would be set to numItems + 1
     */
    val features : Matrix = new DenseMatrix(Util.numItems + 1, Util.numFeatures)    
    /*
     * Add all item-feature-vector to the matrix with index itemID
     */
    while (in.hasNext) {
      val temp = in.next()
      val itemID = temp._3
      val itemFeature = temp._2.get
      features.assignRow(itemID, itemFeature)
    }
    
    val featureMatrix : PactMatrix = new PactMatrix()
    featureMatrix.set(features)
    (Util.numItems, featureMatrix)
  }
}