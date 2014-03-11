package eu.stratosphere.dima.recommendationsystem

import eu.stratosphere.api.scala.functions._
import java.util.Random;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

class InitItemFeatureMatrixMapper extends MapFunction[(Int, PactVector), (Int, PactVector, Int)] {
  override def apply(in : (Int, PactVector)) : (Int, PactVector, Int) = {
    val features : Vector = new SequentialAccessSparseVector(Integer.MAX_VALUE, Util.numFeatures)
    val featureVector : PactVector = new PactVector()
    val numfeatures = Util.numFeatures
    val random : Random = new Random()
    for (i <- 1 to numfeatures) {
      features.set(i, random.nextFloat());
    }
    featureVector.set(features)
    (0, featureVector, in._1)
  }
}