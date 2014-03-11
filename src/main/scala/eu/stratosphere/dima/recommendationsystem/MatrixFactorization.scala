package eu.stratosphere.dima.recommendationsystem;


import org.apache.mahout.math.Vector
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VectorWritable

import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription
import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.scala.TextFile
import eu.stratosphere.api.scala.ScalaPlan
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.client.RemoteExecutor

object RunMatrixFactorizationLocal {
  def main(args: Array[String]) {
    val job = new MatrixFactorization
    
    val inputPath = "file://C:/Users/zhou/Documents/BigDataClass/MatrixFactorization-Scala/datasets/ml-100k/ua.base"
    val outputPath = "file://C:/Users/zhou/Documents/BigDataClass/MatrixFactorization-Scala/results/output.txt"  
    val plan = job.getScalaPlan(1, inputPath, outputPath, 0.1f, 10, 943, 1682, 10)
    LocalExecutor.execute(plan)
    System.exit(0)
  }
}

class MatrixFactorization extends Program with ProgramDescription with Serializable {
  override def getDescription() = {
    "Parameters: [numSubStasks] [input] [output]"
  }
  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1), args(2), args(3).toFloat, args(4).toInt, args(5).toInt, args(6).toInt, args(7).toInt)
  }

  def formatOutput = (word: String, count: Int) => "%s %d".format(word, count)

  def getScalaPlan(numSubTasks:Int, inputPath: String, outputPath: String, lambda: Float, numFeatures: Int, numUsers: Int, numItems: Int, numIter: Int) = {
    println("Job Started")
    println("InputPath: " + inputPath)
    println("OutputPath: " + outputPath)
    Util.setParameters(lambda, numFeatures, numUsers, numItems)
    
    val tupple = DataSource(inputPath, CsvInputFormat[(Int, Int, Float)](Seq(0, 1, 2), "\n", '\t'))
    val itemRatingVectorMap = tupple map {new ItemRatingVectorMapper}
    val itemRatingVectorReduce = itemRatingVectorMap groupBy {case (itemID, _) => itemID} reduceGroup {new ItemRatingVectorReducer}
    
    val userRatingVectorMap = tupple map {new UserRatingVectorMapper}
    val userRatingVectorReduce = userRatingVectorMap groupBy {case (userID, _) => userID} reduceGroup {new UserRatingVectorReducer}
   
    val initItemFeatureMatrixMap = itemRatingVectorReduce map {new InitItemFeatureMatrixMapper}
    val initItemFeatureMatrixReduce = initItemFeatureMatrixMap groupBy {case (flag, _, _) => flag} reduceGroup {new ItemFeatureMatrixReducer}
    
    var userFeatureMatrixCross = userRatingVectorReduce cross initItemFeatureMatrixReduce map {new UserFeatureMatrixCrosser}
    var userFeatureMatrixReduce = userFeatureMatrixCross groupBy {case (flag, _, _) => flag} reduceGroup {new UserFeatureMatrixReducer}
    
    var itemFeatureMatrixCross = userFeatureMatrixCross
    var itemFeatureMatrixReduce = userFeatureMatrixReduce
    
    for (i <- 1 to numIter) {
      itemFeatureMatrixCross = itemRatingVectorReduce cross userFeatureMatrixReduce map {new ItemFeatureMatrixCrosser}
      itemFeatureMatrixReduce = itemFeatureMatrixCross groupBy {case (flag, _, _) => flag} reduceGroup {new ItemFeatureMatrixReducer}
      userFeatureMatrixCross = userRatingVectorReduce cross itemFeatureMatrixReduce map {new UserFeatureMatrixCrosser}
      userFeatureMatrixReduce = userFeatureMatrixCross groupBy {case (flag, _, _) => flag} reduceGroup {new UserFeatureMatrixReducer}
    }
    
    val prediction = itemFeatureMatrixCross cross userFeatureMatrixCross map {new PredicetionCrosser}
    
    val output = prediction.write(outputPath, CsvOutputFormat("\n", "\t"))
  
    val plan = new ScalaPlan(Seq(output), "Rating Prediction Computation")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }
}
