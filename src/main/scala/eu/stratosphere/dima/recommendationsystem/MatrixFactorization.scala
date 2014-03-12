/*
 * Project: MatrixFactorization
 * @author Xugang Zhou
 * @author Fangzhou Yang
 * @version 1.0
 */

package eu.stratosphere.dima.recommendationsystem;

import org.apache.mahout.math.Vector
import org.apache.mahout.math.VectorWritable
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;

import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.client.LocalExecutor

/*
 * This Class is the the "plan" class of this project.
 */
class MatrixFactorization extends Program with ProgramDescription with Serializable {

  override def getDescription() = {

    "Parameters: [numSubStasks] [input] [output] [lambda] [numFeatures] [numUsers] [numItems] [numIterations]"

  }
  override def getPlan(args: String*) = {

    getScalaPlan(args(0).toInt, args(1), args(2), args(3).toFloat, args(4).toInt, args(5).toInt, args(6).toInt, args(7).toInt)

  }
  /*
   * This method defines how the data would be operated.
   * @return The whole scala-plan
   * @param args(0) Number of subtasks to specify parallelism
   * @param args(1) Path to input file
   * @param args(2) Path to output file
   * @param args(3) lambda which is used during ALS learning
   * @param args(4) Number of features pre-set for learning
   * @param args(5) Number of Users of input
   * @param args(6) Number of Items of input
   * @param args(7) Number of Iterations to run in ALS 
   */
  def getScalaPlan(numSubTasks:Int, inputPath: String, outputPath: String, lambda: Float, numFeatures: Int, numUsers: Int, numItems: Int, numIter: Int) = {

    println("Job Started")
    println("InputPath: " + inputPath)
    println("OutputPath: " + outputPath)
 
    Util.setParameters(lambda, numFeatures, numUsers, numItems)
    
    val tupple = DataSource(inputPath, CsvInputFormat[(Int, Int, Float)](Seq(0, 1, 2), "\n", '\t'))

    /*
     * Get item-rating-vector 
     */
    val itemRatingVectorMap = tupple map {new ItemRatingVectorMapper}
    val itemRatingVectorReduce = itemRatingVectorMap groupBy {case (itemID, _) => itemID} reduceGroup {new ItemRatingVectorReducer}
    
    /*
     * Get user-rating-vector
     */
    val userRatingVectorMap = tupple map {new UserRatingVectorMapper}
    val userRatingVectorReduce = userRatingVectorMap groupBy {case (userID, _) => userID} reduceGroup {new UserRatingVectorReducer}
   
    /*
     * Initialize item-feature-matrix with random value
     */
    val initItemFeatureMatrixMap = itemRatingVectorReduce map {new InitItemFeatureMatrixMapper}
    val initItemFeatureMatrixReduce = initItemFeatureMatrixMap groupBy {case (flag, _, _) => flag} reduceGroup {new ItemFeatureMatrixReducer}
    
    /*
     * Learn the user-feature-matrix with initialized item-feature-matrix
     */
    var userFeatureMatrixCross = userRatingVectorReduce cross initItemFeatureMatrixReduce map {new UserFeatureMatrixCrosser}
    var userFeatureMatrixReduce = userFeatureMatrixCross groupBy {case (flag, _, _) => flag} reduceGroup {new UserFeatureMatrixReducer}
    
    /*
     * Learn the item-feature-matrix with user-feature-matrix
     */
    var itemFeatureMatrixCross = itemRatingVectorReduce cross userFeatureMatrixReduce map {new ItemFeatureMatrixCrosser}
    var itemFeatureMatrixReduce = itemFeatureMatrixCross groupBy {case (flag, _, _) => flag} reduceGroup {new ItemFeatureMatrixReducer}
    
    /*
     * Continue to Alternative-Least-Sqaure (ALS) learning with numIter iterations
     */
    for (i <- 1 to numIter) {
      userFeatureMatrixCross = userRatingVectorReduce cross itemFeatureMatrixReduce map {new UserFeatureMatrixCrosser}
      userFeatureMatrixReduce = userFeatureMatrixCross groupBy {case (flag, _, _) => flag} reduceGroup {new UserFeatureMatrixReducer}
      itemFeatureMatrixCross = itemRatingVectorReduce cross userFeatureMatrixReduce map {new ItemFeatureMatrixCrosser}
      itemFeatureMatrixReduce = itemFeatureMatrixCross groupBy {case (flag, _, _) => flag} reduceGroup {new ItemFeatureMatrixReducer}
    }
    
    /*
     * Use learned user- and item-feature-vectors to do the prediction of rating
     */
    val prediction = itemFeatureMatrixCross cross userFeatureMatrixCross map {new PredicetionCrosser}
    
    /*
     * Put the predicted-rating result to output stream
     */
    val output = prediction.write(outputPath, CsvOutputFormat("\n", "\t"))
  
    /*
     * Return the plan
     */
    val plan = new ScalaPlan(Seq(output), "Rating Prediction Computation")
    plan.setDefaultParallelism(numSubTasks)
 
    plan
 
  }
}

/*
 * This object enables you to run this project locally.
 * Run this object with the parameters specified below will result in run this project locally.
 */
object RunMatrixFactorizationLocal {
  
  def main(args: Array[String]) {

    val job = new MatrixFactorization
    
    val plan = job.getScalaPlan(args(0).toInt, args(1), args(2), args(3).toFloat, args(4).toInt, args(5).toInt, args(6).toInt, args(7).toInt)
    LocalExecutor.execute(plan)

    System.exit(0)

  }
}
