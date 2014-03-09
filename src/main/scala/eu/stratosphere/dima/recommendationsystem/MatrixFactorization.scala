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
    val plan = job.getScalaPlan(args(0).toInt, args(1), args(2))
    LocalExecutor.execute(plan)
    System.exit(0)
  }
}

class MatrixFactorization extends Program with ProgramDescription with Serializable {
  override def getDescription() = {
    "Parameters: [numSubStasks] [input] [output]"
  }
  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1), args(2))
  }

  def formatOutput = (word: String, count: Int) => "%s %d".format(word, count)

  def getScalaPlan(numSubTasks: Int, textInput: String, wordsOutput: String) = {
    val input = TextFile(textInput)

    val words = input flatMap { _.toLowerCase().split("""\W+""") filter { _ != "" } map { (_, 1) } }
    val counts = words groupBy { case (word, _) => word } reduce { (w1, w2) => (w1._1, w1._2 + w2._2) }

    counts neglects { case (word, _) => word }
    counts preserves({ case (word, _) => word }, { case (word, _) => word })
    val output = counts.write(wordsOutput, DelimitedOutputFormat(formatOutput.tupled))
  
    val plan = new ScalaPlan(Seq(output), "Word Count (immutable)")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }
}
