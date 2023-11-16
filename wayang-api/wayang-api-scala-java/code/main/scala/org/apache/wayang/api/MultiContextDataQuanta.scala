/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.wayang.api

import org.apache.wayang.api.serialization.SerializationUtils
import org.apache.wayang.basic.operators.{ObjectFileSink, SampleOperator, TextFileSink}
import org.apache.wayang.core.api.exception.WayangException
import org.apache.wayang.core.plan.wayangplan.{Operator, WayangPlan}
import org.apache.wayang.core.util.ReflectionUtils

import java.io.{FileInputStream, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class MultiContextDataQuanta[Out: ClassTag](val dataQuanta: DataQuanta[Out])(val multiContextPlanBuilder: MultiContextPlanBuilder) {

  private def wrapInMultiContextDataQuanta[NewOut: ClassTag](f: DataQuanta[Out] => DataQuanta[NewOut]): MultiContextDataQuanta[NewOut] =
    new MultiContextDataQuanta[NewOut](f(dataQuanta))(this.multiContextPlanBuilder)

  def map[NewOut: ClassTag](udf: Out => NewOut): MultiContextDataQuanta[NewOut] =
    wrapInMultiContextDataQuanta(_.map(udf))

  def mapPartitions[NewOut: ClassTag](udf: Iterable[Out] => Iterable[NewOut]): MultiContextDataQuanta[NewOut] =
    wrapInMultiContextDataQuanta(_.mapPartitions(udf))

  def filter(udf: Out => Boolean): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.filter(udf))

  def flatMap[NewOut: ClassTag](udf: Out => Iterable[NewOut]): MultiContextDataQuanta[NewOut] =
    wrapInMultiContextDataQuanta(_.flatMap(udf))

  def sample(sampleSize: Int,
             datasetSize: Long = SampleOperator.UNKNOWN_DATASET_SIZE,
             seed: Option[Long] = None,
             sampleMethod: SampleOperator.Methods = SampleOperator.Methods.ANY): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.sample(sampleSize, datasetSize, seed, sampleMethod))

  def sampleDynamic(sampleSizeFunction: Int => Int,
                    datasetSize: Long = SampleOperator.UNKNOWN_DATASET_SIZE,
                    seed: Option[Long] = None,
                    sampleMethod: SampleOperator.Methods = SampleOperator.Methods.ANY): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.sampleDynamic(sampleSizeFunction, datasetSize, seed, sampleMethod))

  def reduceByKey[Key: ClassTag](keyUdf: Out => Key,
                                 udf: (Out, Out) => Out): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.reduceByKey(keyUdf, udf))

  def groupByKey[Key: ClassTag](keyUdf: Out => Key): MultiContextDataQuanta[java.lang.Iterable[Out]] =
    wrapInMultiContextDataQuanta(_.groupByKey(keyUdf))

  def reduce(udf: (Out, Out) => Out): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.reduce(udf))

  def union(that: MultiContextDataQuanta[Out]): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.union(that.dataQuanta))

  def intersect(that: MultiContextDataQuanta[Out]): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.intersect(that.dataQuanta))

  import org.apache.wayang.basic.data.{Tuple2 => WayangTuple2}

  def join[ThatOut: ClassTag, Key: ClassTag](thisKeyUdf: Out => Key,
                                             that: MultiContextDataQuanta[ThatOut],
                                             thatKeyUdf: ThatOut => Key)
  : MultiContextDataQuanta[WayangTuple2[Out, ThatOut]] =
    wrapInMultiContextDataQuanta(_.join(thisKeyUdf, that.dataQuanta, thatKeyUdf))

  def coGroup[ThatOut: ClassTag, Key: ClassTag](thisKeyUdf: Out => Key,
                                                that: MultiContextDataQuanta[ThatOut],
                                                thatKeyUdf: ThatOut => Key)
  : MultiContextDataQuanta[WayangTuple2[java.lang.Iterable[Out], java.lang.Iterable[ThatOut]]] =
    wrapInMultiContextDataQuanta(_.coGroup(thisKeyUdf, that.dataQuanta, thatKeyUdf))

  def sort[Key: ClassTag](keyUdf: Out => Key): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.sort(keyUdf))

  def cartesian[ThatOut: ClassTag](that: MultiContextDataQuanta[ThatOut])
  : MultiContextDataQuanta[WayangTuple2[Out, ThatOut]] =
    wrapInMultiContextDataQuanta(_.cartesian(that.dataQuanta))

  def zipWithId: MultiContextDataQuanta[WayangTuple2[java.lang.Long, Out]] =
    wrapInMultiContextDataQuanta(_.zipWithId)

  def distinct: MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.distinct)

  def count: MultiContextDataQuanta[java.lang.Long] =
    wrapInMultiContextDataQuanta(_.count)

  def doWhile[ConvOut: ClassTag](udf: Iterable[ConvOut] => Boolean,
                                 bodyBuilder: DataQuanta[Out] => (DataQuanta[Out], DataQuanta[ConvOut]),
                                 numExpectedIterations: Int = 20)
  : MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.doWhile(udf, bodyBuilder, numExpectedIterations))

  def repeat(n: Int, bodyBuilder: DataQuanta[Out] => DataQuanta[Out]): MultiContextDataQuanta[Out] =
    wrapInMultiContextDataQuanta(_.repeat(n, bodyBuilder))

  def foreach(f: Out => _): Unit =
    dataQuanta.foreach(f)


  def execute(): Unit = {

    val tempFiles: ConcurrentLinkedQueue[Path] = new ConcurrentLinkedQueue[Path]() // To store the temp file names
    val processes: ListBuffer[Process] = ListBuffer() // To store the processes

    // For spawning child process using wayang-submit under wayang home
    val wayangHome = System.getenv("WAYANG_HOME")

    // Write operator to temp file
    val operatorPath = MultiContextDataQuanta.writeToTempFile2(dataQuanta.operator)

    multiContextPlanBuilder.contexts.foreach {
      context =>

        // Write context to temp file
        val multiContextPlanBuilderPath = MultiContextDataQuanta.writeToTempFile2(MultiContextPlanBuilder(List(context)))

        // Store the paths for later deletion
        tempFiles.add(operatorPath)
        tempFiles.add(multiContextPlanBuilderPath)

        println(s"About to start a process with args ${(operatorPath, multiContextPlanBuilderPath)}")

        // Spawn child process
        val processBuilder = new ProcessBuilder(
          s"$wayangHome/bin/wayang-submit",
          "org.apache.wayang.api.MultiContextDataQuanta",
          operatorPath.toString,
          multiContextPlanBuilderPath.toString)

        // Redirect children out to parent out
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT)

        val process = processBuilder.start()  // Start process
        processes += process // Store the process for later
    }

    // Wait for all processes to complete
    processes.foreach(_.waitFor())

    // Delete all temporary files
    tempFiles.forEach(path => Files.deleteIfExists(path))
  }

}


object MultiContextDataQuanta {
  def main(args: Array[String]): Unit = {
    println("New process here")
    println(args.mkString("Array(", ", ", ")"))
    println()

    if (args.length != 2) {
      System.err.println("Expected two arguments: paths to the serialized operator and context.")
      System.exit(1)
    }

    // Parse file paths
    val operatorPath = Path.of(args(0))
    val multiContextPlanBuilderPath = Path.of(args(1))

    // Parse operator and multiContextPlanBuilder
    val operator = MultiContextDataQuanta.readFromTempFile2[Operator](operatorPath)
    val multiContextPlanBuilder = MultiContextDataQuanta.readFromTempFile2[MultiContextPlanBuilder](multiContextPlanBuilderPath)

    // Get context
    val context = multiContextPlanBuilder.contexts.head

    // Get classes of and also add this one
    var withClassesOf = multiContextPlanBuilder.withClassesOf
    withClassesOf = withClassesOf :+ classOf[MultiContextDataQuanta[_]]
    println(s"withClassesOf: $withClassesOf")

    // Connect to sink and execute plan
    context.getSink match {
      case Some(textFileSink: BlossomContext.TextFileSink) =>
        connectToSinkAndExecutePlan(new TextFileSink[AnyRef](textFileSink.textFileUrl, classOf[AnyRef]))

      case Some(objectFileSink: BlossomContext.ObjectFileSink) =>
        connectToSinkAndExecutePlan(new ObjectFileSink[AnyRef](objectFileSink.textFileUrl, classOf[AnyRef]))

      case None =>
        throw new WayangException("All contexts must be attached to an output sink.")

      case _ =>
        throw new WayangException("Invalid sink..")
    }

    def connectToSinkAndExecutePlan(sink: Operator): Unit = {
      operator.connectTo(0, sink, 0)
      context.execute(new WayangPlan(sink), withClassesOf.map(ReflectionUtils.getDeclaringJar).filterNot(_ == null): _*)
    }
  }

  def writeToTempFile(obj: AnyRef): Path = {
    val tempFile = Files.createTempFile("serialized", ".tmp")
    val fos = new FileOutputStream(tempFile.toFile)
    try {
      fos.write(SerializationUtils.serialize(obj))
    } finally {
      fos.close()
    }
    tempFile
  }

  def readFromTempFile[T : ClassTag](path: Path): T = {
    val fis = new FileInputStream(path.toFile)
    try {
      SerializationUtils.deserialize[T](fis.readAllBytes())
    } finally {
      fis.close()
      Files.deleteIfExists(path)
    }
  }

  def writeToTempFile2(obj: AnyRef): Path = {
    val tempFile = Files.createTempFile("serialized", ".tmp")
    println(s"Just created temp file ${tempFile.toFile.getName}")
    val serializedString = SerializationUtils.serializeAsString(obj)
    Files.writeString(tempFile, serializedString, StandardCharsets.UTF_8)
    tempFile
  }

  def readFromTempFile2[T: ClassTag](path: Path): T = {
    val serializedString = Files.readString(path, StandardCharsets.UTF_8)
    val deserializedObject = SerializationUtils.deserializeFromString[T](serializedString)
    Files.deleteIfExists(path)
    deserializedObject
  }

}
