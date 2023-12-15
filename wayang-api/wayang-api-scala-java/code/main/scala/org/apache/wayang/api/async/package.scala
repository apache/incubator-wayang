package org.apache.wayang.api

import org.apache.logging.log4j.{LogManager, Logger}

import java.nio.file.Files
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

package object async {

  val logger: Logger = LogManager.getLogger(getClass)

  def runAsyncWithTempFileOut[Out: ClassTag](dataQuanta: DataQuanta[Out], tempFileOut: String): Future[DataQuantaAsyncResult[Out]] = {
    runAsyncWithObjectFileOut(dataQuanta, tempFileOut)
      .map(_ => DataQuantaAsyncResult(tempFileOut, implicitly[ClassTag[Out]]))
  }

  def runAsyncWithTextFileOut[Out: ClassTag](dataQuanta: DataQuanta[Out], textFileOut: String): Future[Unit] = {
    val wayangContext = dataQuanta.planBuilder.wayangContext
    wayangContext match {
      case context: BlossomContext =>
        val updatedContext = context.withTextFileSink(textFileOut)
        runAsyncBody(dataQuanta, updatedContext)
      case _ =>
        throw new Exception("WayangContext is not of type BlossomContext")
    }
  }

  def runAsyncWithObjectFileOut[Out: ClassTag](dataQuanta: DataQuanta[Out], url: String): Future[Unit] = {
    val wayangContext = dataQuanta.planBuilder.wayangContext
    wayangContext match {
      case context: BlossomContext =>
        val updatedContext = context.withObjectFileSink(url)
        runAsyncBody(dataQuanta, updatedContext)
      case _ =>
        throw new Exception("WayangContext is not of type BlossomContext")
    }
  }

  private def runAsyncBody[Out: ClassTag](dataQuanta: DataQuanta[Out], blossomContext: BlossomContext): Future[Unit] = Future {

    // Write context to temp file
    // TODO: UDF Jars?
    val multiContextPlanBuilderPath = MultiContextDataQuanta.writeToTempFileAsString(
      new MultiContextPlanBuilder(List(blossomContext)).withUdfJarsOf(classOf[DataQuantaImplicits]).withUdfJars(dataQuanta.planBuilder.udfJars.toSeq: _*)
    )

    // Write operator to temp file
    val operatorPath = MultiContextDataQuanta.writeToTempFileAsString(dataQuanta.operator)

    // Child process
    logger.info("About to start a process with args ${(operatorPath, multiContextPlanBuilderPath)}")
    val wayangHome = System.getenv("WAYANG_HOME")
    val processBuilder = new ProcessBuilder(
      s"$wayangHome/bin/wayang-submit",
      "org.apache.wayang.api.MultiContextDataQuanta",
      operatorPath.toString,
      multiContextPlanBuilderPath.toString)

    // Redirect children out to parent out
    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
    processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT)

    // Start child process
    val process = processBuilder.start()

    // And block this future while waiting for it
    process.waitFor()

    // Delete temp files
    Files.deleteIfExists(multiContextPlanBuilderPath)
    Files.deleteIfExists(operatorPath)

  }

}
