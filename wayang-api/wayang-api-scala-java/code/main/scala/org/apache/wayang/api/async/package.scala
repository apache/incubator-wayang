package org.apache.wayang.api

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.wayang.api.serialization.TempFileUtils
import org.apache.wayang.core.api.exception.WayangException

import java.nio.file.Files
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

package object async {

  val logger: Logger = LogManager.getLogger(getClass)

  /**
   * Runs the given data quanta asynchronously with a temporary file as the output.
   *
   * @param dataQuanta  The data quanta to be executed.
   * @param tempFileOut The path to the temporary output file.
   * @tparam Out The type parameter of the data quanta and the output.
   * @return A future representing the completion of the execution, which will contain a
   *         DataQuantaAsyncResult object that holds the path to the temporary output file
   *         and the class tag for the output type.
   */
  def runAsyncWithTempFileOut[Out: ClassTag](dataQuanta: DataQuanta[Out], tempFileOut: String): Future[DataQuantaAsyncResult[Out]] = {
    runAsyncWithObjectFileOut(dataQuanta, tempFileOut)
      .map(_ => DataQuantaAsyncResult(tempFileOut, implicitly[ClassTag[Out]]))
  }


  /**
   * Runs the given DataQuanta asynchronously and writes the output to a text file.
   *
   * @param dataQuanta the DataQuanta to be executed
   * @param url        the URL of the text file to write the output to
   * @tparam Out the type of the output DataQuanta
   * @return a Future representing the completion of the execution
   * @throws WayangException if the WayangContext is not of type BlossomContext
   */
  def runAsyncWithTextFileOut[Out: ClassTag](dataQuanta: DataQuanta[Out], url: String): Future[Unit] = {
    val wayangContext = dataQuanta.planBuilder.wayangContext
    wayangContext match {
      case context: BlossomContext =>
        val updatedContext = context.withTextFileSink(url)
        runAsyncBody(dataQuanta, updatedContext)
      case _ =>
        throw new WayangException("WayangContext is not of type BlossomContext")
    }
  }


  /**
   * Runs the given DataQuanta asynchronously and writes the output to an object file specified by the URL.
   *
   * @param dataQuanta the DataQuanta to be executed
   * @param url        the URL of the object file to write the output to
   * @tparam Out the type parameter for the output DataQuanta
   * @return a Future that represents the execution of the DataQuanta
   * @throws WayangException if the WayangContext is not of type BlossomContext
   */
  def runAsyncWithObjectFileOut[Out: ClassTag](dataQuanta: DataQuanta[Out], url: String): Future[Unit] = {
    val wayangContext = dataQuanta.planBuilder.wayangContext
    wayangContext match {
      case context: BlossomContext =>
        val updatedContext = context.withObjectFileSink(url)
        runAsyncBody(dataQuanta, updatedContext)
      case _ =>
        throw new WayangException("WayangContext is not of type BlossomContext")
    }
  }


  def runAsyncBody[Out: ClassTag](dataQuanta: DataQuanta[Out], blossomContext: BlossomContext): Future[Unit] = Future {

    import scala.concurrent.blocking

    val planBuilderPath = TempFileUtils.writeToTempFileAsString(dataQuanta.planBuilder.withUdfJarsOf(this.getClass))

    // Write operator to temp file
    val operatorPath = TempFileUtils.writeToTempFileAsString(dataQuanta.operator)

    var process: Process = null

    try {
      val wayangHome = Option(System.getenv("WAYANG_HOME"))
        .getOrElse(throw new RuntimeException("WAYANG_HOME is not set in the environment"))
      val wayangSubmit = s"$wayangHome/bin/wayang-submit"
      val mainClass = "org.apache.wayang.api.async.Main"

      // Child process
      val processBuilder = new ProcessBuilder(
        wayangSubmit,
        mainClass,
        operatorPath.toString,
        planBuilderPath.toString)

      // Redirect children out to parent out
      processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
      processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT)

      // Start child process
      process = processBuilder.start()

      // And block this future while waiting for it
      blocking {
        process.waitFor()
      }
    }

    finally {
      Files.deleteIfExists(planBuilderPath)
      Files.deleteIfExists(operatorPath)
    }

  }

}
