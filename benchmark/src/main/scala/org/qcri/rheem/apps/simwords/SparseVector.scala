package org.qcri.rheem.apps.simwords

import scala.util.Random

/**
  * A [[SparseVector]] is a vector of arbitrary dimension. It does not store `0` components.
  */
case class SparseVector(indices: Array[Int], values: Array[Double]) {

  /**
    * Applies scalar to this vector.
    */
  def *=(scalar: Double): Unit = {
    for (i <- this.values.indices) {
      values(i) *= scalar
    }
  }

  /**
    * @return the length of this vector
    */
  def length: Double = math.sqrt(this.values.map(v => v * v).sum)

  /**
    * Keep the direction but bring to length 1.
    */
  def normalize(): Unit = {
    val scalar = 1d / length
    this *= scalar
  }

  /**
    * Add two vectors.
    */
  def +(that: SparseVector): SparseVector = {
    val newIndices: Array[Int] = new Array[Int](this.indices.length + that.indices.length)
    val newValues: Array[Double] = new Array[Double](this.values.length + that.values.length)
    var newI = 0

    // Co-iterate the lists.
    var (thisI, thatI) = (0, 0)
    while (thisI < this.indices.length && thatI < that.indices.length) {
      val (thisIndex, thatIndex) = (this.indices(thisI), that.indices(thatI))
      if (thisIndex < thatIndex) {
        newIndices(newI) = thisIndex
        newValues(newI) = this.values(thisI)
        thisI += 1
      } else if (thisIndex > thatIndex) {
        newIndices(newI) = thatIndex
        newValues(newI) = that.values(thatI)
        thatI += 1
      } else {
        newIndices(newI) = thisIndex
        newValues(newI) = this.values(thisI) + that.values(thatI)
        thisI += 1
        thatI += 1
      }
      newI += 1
    }

    // Append trailing list tail.
    while (thisI < this.indices.length) {
      newIndices(newI) = this.indices(thisI)
      newValues(newI) = this.values(thisI)
      thisI += 1
      newI += 1
    }
    while (thatI < that.indices.length) {
      newIndices(newI) = that.indices(thatI)
      newValues(newI) = that.values(thatI)
      thatI += 1
      newI += 1
    }

    // Build the new instance.
    if (newI < newIndices.length) {
      SparseVector(java.util.Arrays.copyOfRange(newIndices, 0, newI),
        java.util.Arrays.copyOfRange(newValues, 0, newI))
    } else {
      SparseVector(newIndices, newValues)
    }
  }

  /**
    * Dot-product of two sparse vectors.
    */
  def *(that: SparseVector): Double = {
    var prodSum = 0d;

    // Co-iterate the lists.
    var (thisI, thatI) = (0, 0)
    while (thisI < this.indices.length && thatI < that.indices.length) {
      val (thisIndex, thatIndex) = (this.indices(thisI), that.indices(thatI))
      if (thisIndex < thatIndex) {
        thisI += 1
      } else if (thisIndex > thatIndex) {
        thatI += 1
      } else {
        prodSum += this.values(thisI) * that.values(thatI)
        thisI += 1
        thatI += 1
      }
    }

    prodSum
  }

  /**
    * Cosine similarity of two sparse vectors.
    */
  def ~(that: SparseVector): Double = {
    math.abs(this * that) / (this.length * that.length)
  }

  override def toString = s"(${
    this.indices
      .zip(this.values)
      .map(component => s"${component._1}->${component._2}")
      .mkString(", ")
  })"

  def toDictionaryString = s"{${
    this.indices
      .zip(this.values)
      .map(component => s"${component._1}:${component._2}")
      .mkString(",")
  }}"

  override def hashCode = java.util.Arrays.hashCode(this.values) ^ java.util.Arrays.hashCode(this.indices)

  override def equals(o: Any): Boolean = {
    if (o == null || !o.isInstanceOf[SparseVector]) return false;
    val that = o.asInstanceOf[SparseVector]
    java.util.Arrays.equals(this.indices, that.indices) && java.util.Arrays.equals(this.values, that.values)
  }


}

/**
  * Companion object for [[SparseVector]].
  */
object SparseVector {

  private lazy val random = new Random

  /**
    * Create a random instance.
    *
    * @param indices          possible [[SparseVector#indices]]
    * @param probCompleteness probability of using all components in `numCreations`
    * @param numCreations     number of [[SparseVector]]s that will be created
    */
  def createRandom(indices: Array[Int], probCompleteness: Double, numCreations: Int, withNegative: Boolean = false) = {
    // Find out the probability to include each component into the vector.
    val probPickElement = calculatePickElementProb(probCompleteness, numCreations)

    // Generate the components.
    val builder = new Builder
    for (i <- indices.indices) {
      if (probPickElement >= this.random.nextDouble()) {
        builder.add(indices(i), this.random.nextDouble() * (if (withNegative && this.random.nextBoolean()) -1 else 1))
      }
    }

    // Build the vector.
    builder.build
  }

  /**
    * Estimate the required probability to pick an element in a single try, given a the probability of picking all
    * elements in a number of repetitions.
    */
  private def calculatePickElementProb(probCompleteness: Double, numCreations: Int) =
    1 - math.pow(1 - probCompleteness, 1d / numCreations)

  class Builder {

    private val components = scala.collection.mutable.Map[Int, Double]()

    /**
      * Add a delta to a component.
      */
    def add(dimension: Int, delta: Double): Builder = {
      val newValue = components.getOrElse(dimension, 0d) + delta
      if (newValue == 0) this.components.remove(dimension)
      else this.components.update(dimension, newValue)
      this
    }

    /**
      * Create the [[SparseVector]].
      */
    def build: SparseVector = {
      val indices = new Array[Int](this.components.size)
      val values = new Array[Double](this.components.size)
      var i = 0
      for (component <- this.components.toArray.sortBy(_._1)) {
        indices(i) += component._1
        values(i) += component._2
        i += 1
      }
      SparseVector(indices, values)
    }

    /**
      * Tells whether this instance contains components.
      */
    def isEmpty = components.isEmpty

  }

}