package eu.stratosphere.dima.recommendationsystem

import eu.stratosphere.api.scala.functions._
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.als.AlternatingLeastSquaresSolver;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import java.util.List;


import com.google.common.collect.Lists;

class ItemFeatureMatrixCrosser extends CrossFunction[(Int, PactVector), (Int, PactMatrix), (Int, PactVector, Int)] {
  override def apply(l: (Int, PactVector), r: (Int, PactMatrix)): (Int, PactVector, Int) = {
    val itemID = l._1
    val itemVector : Vector = l._2.get
    val numUsers = r._1
    val userMatrix : Matrix = r._2.get
    val userFeatureMatrix : OpenIntObjectHashMap[Vector] = new OpenIntObjectHashMap[Vector](numUsers)
    for (i <- 1 to Util.maxUserID) {
      val userVector : Vector  = userMatrix.viewRow(i)
      if (userVector.getNumNondefaultElements() > 0) {
        userFeatureMatrix.put(i, userVector)
      }
    }
    val featureVectors : List[Vector] = Lists.newArrayListWithCapacity(itemVector.getNumNondefaultElements())
    var temp = itemVector.nonZeroes().iterator()
    while (temp.hasNext()) {
      val e = temp.next()
      val index = e.index()
       if(userFeatureMatrix.containsKey(index)) {
         featureVectors.add(userFeatureMatrix.get(index));
       }
    }
    val userFeatureVector : Vector = AlternatingLeastSquaresSolver.solve(featureVectors, itemVector, Util.lambda, Util.numFeatures)
    val userFeatureVectorWritable : PactVector = new PactVector()
    userFeatureVectorWritable.set(userFeatureVector)
    (0, userFeatureVectorWritable, itemID)
  }
}