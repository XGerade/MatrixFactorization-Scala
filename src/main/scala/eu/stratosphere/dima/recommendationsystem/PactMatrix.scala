package eu.stratosphere.dima.recommendationsystem

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;

import eu.stratosphere.types.Value

class PactMatrix extends Value {
  
  val matrixWritable : MatrixWritable = new MatrixWritable
  
  def set(v: Matrix) {
    matrixWritable.set(v)
  }
  
  def get : Matrix = {
    val result = matrixWritable.get()
    result
  }
  
  override def read(in: DataInput) {
    matrixWritable.readFields(in)
  }
  
  override def write(out: DataOutput) {
    matrixWritable.write(out)
  }
  
  override def toString : String = {
    val result = matrixWritable.toString()
    result
  }
}
