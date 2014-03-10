package eu.stratosphere.dima.recommendationsystem

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;

import eu.stratosphere.types.Value

class PactMatrix extends Value {
  
  val vectorWritable : VectorWritable = new VectorWritable
  
  def set(v: Vector) {
    vectorWritable.set(v)
  }
  
  def get : Vector = {
    val result = vectorWritable.get()
    result
  }
  
  override def read(in: DataInput) {
    vectorWritable.readFields(in)
  }
  
  override def write(out: DataOutput) {
    vectorWritable.write(out)
  }
  
  override def toString : String = {
    val result = vectorWritable.toString()
    result
  }
}
