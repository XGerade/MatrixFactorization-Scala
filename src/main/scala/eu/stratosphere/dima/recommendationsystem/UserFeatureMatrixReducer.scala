package eu.stratosphere.dima.recommendationsystem

import eu.stratosphere.api.scala.functions._
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;


class UserFeatureMatrixReducer extends GroupReduceFunction[(Int, PactVector, Int), (Int, PactMatrix)] {
  override def apply(in: Iterator[(Int, PactVector, Int)]) : (Int, PactMatrix) = {
    val features : Matrix = new SparseMatrix(Util.numUsers, Util.numFeatures)
    
    while (in.hasNext) {
      val temp = in.next()
      val userID = temp._3
      val itemFeature = temp._2.get
      features.assignRow(userID, itemFeature)
    }
    
    val featureMatrix : PactMatrix = new PactMatrix()
    featureMatrix.set(features)
    (Util.numUsers, featureMatrix)
  }
}