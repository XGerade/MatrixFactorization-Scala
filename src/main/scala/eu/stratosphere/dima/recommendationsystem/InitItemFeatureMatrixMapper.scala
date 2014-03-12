/*
 * Project: MatrixFactorization
 * @author Xugang Zhou
 * @author Fangzhou Yang
 * @version 1.0
 */

package eu.stratosphere.dima.recommendationsystem

import eu.stratosphere.api.scala.functions._
import java.util.Random;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

/*
 * This Map Class maps the each itemID to a random initial feature-vector for the itemID
 */
class InitItemFeatureMatrixMapper extends MapFunction[(Int, PactVector), (Int, PactVector, Int)] {

  /*
   * This override method defines how the random feature-vector is generated
   * @param in:(itemID, rating-vector)
   * @return (0, feature-vector, itemID) In the following step, all the feature-vector would be reduced to a feature-matrix
   * So the first field of the output would be 0 for the reduce operation's convenience
   */
  override def apply(in : (Int, PactVector)) : (Int, PactVector, Int) = {
    
    /*
     * FeatureID could start from 0
     * So the vector will be initialize as length numFeatures with cardinality at numFeatures
     */
    val features : Vector = new SequentialAccessSparseVector(Util.numFeatures, Util.numFeatures)
    val featureVector : PactVector = new PactVector()
    
    /*
     * Set random float for each feature
     */
    val numfeatures = Util.numFeatures
    val random : Random = new Random()
    for (i <- 1 to numfeatures) {
      features.set(i - 1, random.nextFloat());
    }
    
    featureVector.set(features)
    (0, featureVector, in._1)
  }
}