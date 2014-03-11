package eu.stratosphere.dima.recommendationsystem

import eu.stratosphere.api.scala.functions._
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.als.AlternatingLeastSquaresSolver;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import java.util.List;


import com.google.common.collect.Lists;

class UserFeatureMatrixCrosser extends CrossFunction[(Int, PactVector), (Int, PactMatrix), (Int, PactVector, Int)] {
  override def apply(l: (Int, PactVector), r: (Int, PactMatrix)): (Int, PactVector, Int) = {
    val userID = l._1
    val userVector : Vector = l._2.get
    val numItems = r._1
    val itemMatrix : Matrix = r._2.get
    val itemFeatureMatrix : OpenIntObjectHashMap[Vector] = new OpenIntObjectHashMap[Vector](numItems)
    for (i <- 1 to Util.maxItemID) {
      val itemVector : Vector  = itemMatrix.viewRow(i)
      if (itemVector.getNumNondefaultElements() > 0) {
        itemFeatureMatrix.put(i, itemVector)
      }
    }
    val featureVectors : List[Vector] = Lists.newArrayListWithCapacity(userVector.getNumNondefaultElements())
    var temp = userVector.nonZeroes().iterator()
    while (temp.hasNext()) {
      val e = temp.next()
      val index = e.index()
       if(itemFeatureMatrix.containsKey(index)) {
         featureVectors.add(itemFeatureMatrix.get(index));
       }
    }
    val itemFeatureVector : Vector = AlternatingLeastSquaresSolver.solve(featureVectors, userVector, Util.lambda, Util.numFeatures)
    val itemFeatureVectorWritable : PactVector = new PactVector()
    itemFeatureVectorWritable.set(itemFeatureVector)
    (0, itemFeatureVectorWritable, userID)
  }
}