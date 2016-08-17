package org.qcri.rheem.api

import java.util

import de.hpi.isg.profiledb.store.model.Experiment
import org.qcri.rheem.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableFunction}
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator
import org.qcri.rheem.core.optimizer.costs.LoadEstimator
import org.qcri.rheem.core.platform.Platform
import org.qcri.rheem.core.types.DataSetType
import org.qcri.rheem.core.util.{Logging, ReflectionUtils}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * Abstract base class for builders of [[DataQuanta]]. The purpose of the builders is to provide a convenient
  * Java API for Rheem that compensates for lacking default and named arguments.
  */
abstract class DataQuantaBuilder[This <: DataQuantaBuilder[_, _], Out] extends Logging {

  /**
    * Lazy-initialized. The [[DataQuanta]] product of this builder.
    */
  private var result: DataQuanta[Out] = _

  /**
    * A name for the [[DataQuanta]] to be built.
    */
  private var name: String = _

  /**
    * An [[Experiment]] for the [[DataQuanta]] to be built.
    */
  private var experiment: Experiment = _

  /**
    * Broadcasts for the [[DataQuanta]] to be built.
    */
  private val broadcasts: ListBuffer[(String, DataQuantaBuilder[_, _])] = ListBuffer()

  /**
    * [[CardinalityEstimator]] for the [[DataQuanta]] to be built.
    */
  private var cardinalityEstimator: CardinalityEstimator = _

  /**
    * Target [[Platform]]s for the [[DataQuanta]] to be built.
    */
  private val targetPlatforms: ListBuffer[Platform] = ListBuffer()

  /**
    * Paths of UDF JAR files for the [[DataQuanta]] to be built.
    */
  private val udfJars: ListBuffer[String] = ListBuffer()

  /**
    * The type of the [[DataQuanta]] to be built.
    */
  protected var outputType: DataSetType[Out] = DataSetType.none().asInstanceOf[DataSetType[Out]]

  /**
    * Set a name for the [[DataQuanta]] and its associated [[org.qcri.rheem.core.plan.rheemplan.Operator]]s.
    *
    * @param name the name
    * @return this instance
    */
  def withName(name: String): This = {
    this.name = name
    this.asInstanceOf[This]
  }

  /**
    * Set an [[Experiment]] for the currently built [[org.qcri.rheem.core.api.Job]].
    *
    * @param experiment the experiment
    * @return this instance
    */
  def withExperiment(experiment: Experiment): This = {
    this.experiment = experiment
    this.asInstanceOf[This]
  }

  def withOutputType(outputType: DataSetType[Out]) = {
    this.outputType = outputType
    this
  }

  def withOutputClass(cls: Class[Out]) = this.withOutputType(DataSetType.createDefault(cls))

  def withBroadcast[Sender <: DataQuantaBuilder[_, _]](sender: Sender, broadcastName: String): This = {
    this.broadcasts += Tuple2(broadcastName, sender)
    this.asInstanceOf[This]
  }

  def withCardinalityEstimator(cardinalityEstimator: CardinalityEstimator): This = {
    this.cardinalityEstimator = cardinalityEstimator
    this.asInstanceOf[This]
  }

  def withTargetPlatform(platform: Platform): This = {
    this.targetPlatforms += platform
    this.asInstanceOf[This]
  }

  def withUdfJarOf(cls: Class[_]): This = this.withUdfJar(ReflectionUtils.getDeclaringJar(cls))

  def withUdfJar(path: String): This = {
    this.udfJars += path
    this.asInstanceOf[This]
  }

  protected implicit def classTag: ClassTag[Out] = ClassTag(outputType.getDataUnitType.getTypeClass)

  def map[NewOut](udf: SerializableFunction[Out, NewOut]) = new MapDataQuantaBuilder(this, udf)

  def globalReduce(udf: SerializableBinaryOperator[Out]) = new GlobalReduceDataQuantaBuilder(this, udf)

  //  def reduceBy[Key](keyUdf: SerializableFunction[Out, Key], udf: SerializableBinaryOperator[Out]) =
  //    new DataQuantaBuilder[_, Out] {
  //      override def build: DataQuanta[Out] = ???
  //    }

  def collect(jobName: String): util.Collection[Out] = {
    import scala.collection.JavaConversions._
    this.dataQuanta().collect(jobName)
  }

  protected[api] def dataQuanta(): DataQuanta[Out] = {
    if (this.result == null) {
      this.result = this.build
      if (this.name != null) this.result.withName(this.name)
      if (this.cardinalityEstimator != null) this.result.withCardinalityEstimator(this.cardinalityEstimator)
      if (this.experiment != null) this.result.withExperiment(experiment)
      this.result.withUdfJars(this.udfJars: _*)
      this.result.withTargetPlatforms(this.targetPlatforms: _*)
      this.broadcasts.foreach {
        case (broadcastName, senderBuilder) => this.result.withBroadcast(senderBuilder.dataQuanta(), broadcastName)
      }
    }
    this.result
  }

  def build: DataQuanta[Out]

}

class LoadCollectionDataQuantaBuilder[Out](collection: util.Collection[Out],
                                           javaPlanBuilder: JavaPlanBuilder)
  extends DataQuantaBuilder[LoadCollectionDataQuantaBuilder[Out], Out] {

  override def build: DataQuanta[Out] = javaPlanBuilder.planBuilder.loadCollection(collection)(this.classTag)

}

class MapDataQuantaBuilder[In, Out](inputDataQuanta: DataQuantaBuilder[_, In],
                                    function: SerializableFunction[In, Out])
  extends DataQuantaBuilder[MapDataQuantaBuilder[In, Out], Out] {

  private var udfCpuEstimator: LoadEstimator = _

  private var udfRamEstimator: LoadEstimator = _

  locally {
    val parameters = ReflectionUtils.getTypeParameters(function.getClass, classOf[SerializableFunction[_, _]])
    val value = parameters.get("Input")
    value match {
      case cls: Class[Out] => this.outputType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", function)
    }
  }

  def withUdfCpuEstimator(udfCpuEstimator: LoadEstimator) = {
    this.udfCpuEstimator = udfCpuEstimator;
    this
  }

  def withUdfRamEstimator(udfRamEstimator: LoadEstimator) = {
    this.udfRamEstimator = udfRamEstimator;
    this
  }

  override def build() = {
    inputDataQuanta.dataQuanta().mapJava(function, this.udfCpuEstimator, this.udfRamEstimator)
  }

}


class GlobalReduceDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T],
                                       udf: SerializableBinaryOperator[T])
  extends DataQuantaBuilder[GlobalReduceDataQuantaBuilder[T], T] {

  private var udfCpuEstimator: LoadEstimator = _

  private var udfRamEstimator: LoadEstimator = _

  def withUdfCpuEstimator(udfCpuEstimator: LoadEstimator) = {
    this.udfCpuEstimator = udfCpuEstimator;
    this
  }

  def withUdfRamEstimator(udfRamEstimator: LoadEstimator) = {
    this.udfRamEstimator = udfRamEstimator;
    this
  }

  override def build() = {
    inputDataQuanta.dataQuanta().reduceJava(udf, this.udfCpuEstimator, this.udfRamEstimator)
  }

}
