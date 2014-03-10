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
    if (args.size < 3) {
      println(job.getDescription)
      return
    }
    val plan = job.getScalaPlan(args(0).toInt, args(1), args(2), args(3).toFloat, args(4).toInt, args(5).toInt, args(6).toInt, args(7).toInt)
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
    
  }
}
