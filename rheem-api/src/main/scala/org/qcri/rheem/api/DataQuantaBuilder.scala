package org.qcri.rheem.api


import java.util.{Collection => JavaCollection}

import de.hpi.isg.profiledb.store.model.Experiment
import org.qcri.rheem.api.util.TypeTrap
import org.qcri.rheem.basic.operators.{GlobalReduceOperator, LocalCallbackSink, MapOperator}
import org.qcri.rheem.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableFunction}
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator
import org.qcri.rheem.core.optimizer.costs.LoadEstimator
import org.qcri.rheem.core.plan.rheemplan.RheemPlan
import org.qcri.rheem.core.platform.Platform
import org.qcri.rheem.core.types.DataSetType
import org.qcri.rheem.core.util.{Logging, ReflectionUtils, RheemCollections}

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
  protected[api] val outputTypeTrap = getOutputTypeTrap

  /**
    * Retrieve an intialization value for [[outputTypeTrap]].
    *
    * @return the [[TypeTrap]]
    */
  protected def getOutputTypeTrap = new TypeTrap

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
    * @param experiment the [[Experiment]]
    * @return this instance
    */
  def withExperiment(experiment: Experiment): This = {
    this.experiment = experiment
    this.asInstanceOf[This]
  }

  /**
    * Explicitly set an output [[DataSetType]] for the currently built [[DataQuanta]]. Note that it is not
    * always necessary to set it and that it can be inferred in some situations.
    *
    * @param outputType the output [[DataSetType]]
    * @return this instance
    */
  def withOutputType(outputType: DataSetType[Out]) = {
    this.outputTypeTrap.dataSetType = outputType
    this
  }

  /**
    * Explicitly set an output [[Class]] for the currently built [[DataQuanta]]. Note that it is not
    * always necessary to set it and that it can be inferred in some situations.
    *
    * @param cls the output [[Class]]
    * @return this instance
    */
  def withOutputClass(cls: Class[Out]) = this.withOutputType(DataSetType.createDefault(cls))

  /**
    * Register a broadcast with the [[DataQuanta]] to be built
    *
    * @param sender        a [[DataQuantaBuilder]] constructing the broadcasted [[DataQuanta]]
    * @param broadcastName the name of the broadcast
    * @return this instance
    */
  def withBroadcast[Sender <: DataQuantaBuilder[_, _]](sender: Sender, broadcastName: String): This = {
    this.broadcasts += Tuple2(broadcastName, sender)
    this.asInstanceOf[This]
  }

  /**
    * Set a [[CardinalityEstimator]] for the currently built [[DataQuanta]].
    *
    * @param cardinalityEstimator the [[CardinalityEstimator]]
    * @return this instance
    */
  def withCardinalityEstimator(cardinalityEstimator: CardinalityEstimator): This = {
    this.cardinalityEstimator = cardinalityEstimator
    this.asInstanceOf[This]
  }

  /**
    * Add a target [[Platform]] on which the currently built [[DataQuanta]] should be calculated. Can be invoked
    * multiple times to set multiple possilbe target [[Platform]]s or not at all to impose no restrictions.
    *
    * @param platform the [[CardinalityEstimator]]
    * @return this instance
    */
  def withTargetPlatform(platform: Platform): This = {
    this.targetPlatforms += platform
    this.asInstanceOf[This]
  }

  /**
    * Register the JAR file containing the given [[Class]] with the currently built [[org.qcri.rheem.core.api.Job]].
    *
    * @param cls the [[Class]]
    * @return this instance
    */
  def withUdfJarOf(cls: Class[_]): This = this.withUdfJar(ReflectionUtils.getDeclaringJar(cls))

  /**
    * Register a JAR file with the currently built [[org.qcri.rheem.core.api.Job]].
    *
    * @param path the path of the JAR file
    * @return this instance
    */
  def withUdfJar(path: String): This = {
    this.udfJars += path
    this.asInstanceOf[This]
  }

  /**
    * Provide a [[ClassTag]] for the constructed [[DataQuanta]].
    *
    * @return the [[ClassTag]]
    */
  protected implicit def classTag: ClassTag[Out] = ClassTag(outputTypeTrap.typeClass)

  /**
    * Feed the built [[DataQuanta]] into a [[MapOperator]].
    *
    * @param udf the UDF for the [[MapOperator]]
    * @return a [[MapDataQuantaBuilder]]
    */
  def map[NewOut](udf: SerializableFunction[Out, NewOut]) = new MapDataQuantaBuilder(this, udf)

  /**
    * Feed the built [[DataQuanta]] into a [[GlobalReduceOperator]].
    *
    * @param udf the UDF for the [[GlobalReduceOperator]]
    * @return a [[GlobalReduceDataQuantaBuilder]]
    */
  def globalReduce(udf: SerializableBinaryOperator[Out]) = new GlobalReduceDataQuantaBuilder(this, udf)

  //  def reduceBy[Key](keyUdf: SerializableFunction[Out, Key], udf: SerializableBinaryOperator[Out]) =
  //    new DataQuantaBuilder[_, Out] {
  //      override def build: DataQuanta[Out] = ???
  //    }

  /**
    * Feed the built [[DataQuanta]] into a [[LocalCallbackSink]] that collects all data quanta locally. This triggers
    * execution of the constructed [[RheemPlan]].
    *
    * @param jobName optional name for the [[RheemPlan]]
    * @return the collected data quanta
    */
  def collect(jobName: String): JavaCollection[Out] = {
    import scala.collection.JavaConversions._
    this.dataQuanta().collect(jobName)
  }

  /**
    * Get or create the [[DataQuanta]] built by this instance.
    *
    * @return the [[DataQuanta]]
    */
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

  /**
    * Create the [[DataQuanta]] built by this instance. Note the configuration being done in [[dataQuanta()]].
    *
    * @return the created and partially configured [[DataQuanta]]
    */
  protected def build: DataQuanta[Out]

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.qcri.rheem.basic.operators.CollectionSource]]s.
  *
  * @param collection      the [[JavaCollection]] to be loaded
  * @param javaPlanBuilder the [[JavaPlanBuilder]]
  */
class LoadCollectionDataQuantaBuilder[Out](collection: JavaCollection[Out],
                                           javaPlanBuilder: JavaPlanBuilder)
  extends DataQuantaBuilder[LoadCollectionDataQuantaBuilder[Out], Out] {

  // Try to infer the type class from the collection.
  locally {
    if (!collection.isEmpty) {
      val any = RheemCollections.getAny(collection)
      if (any != null) {
        this.outputTypeTrap.dataSetType = DataSetType.createDefault(any.getClass)
      }
    }
  }

  override protected def build: DataQuanta[Out] = javaPlanBuilder.planBuilder.loadCollection(collection)(this.classTag)

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.qcri.rheem.basic.operators.MapOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param udf             UDF for the [[MapOperator]]
  */
class MapDataQuantaBuilder[In, Out](inputDataQuanta: DataQuantaBuilder[_, In],
                                    udf: SerializableFunction[In, Out])
  extends DataQuantaBuilder[MapDataQuantaBuilder[In, Out], Out] {

  /** [[LoadEstimator]] to estimate the CPU load of the [[udf]]. */
  private var udfCpuEstimator: LoadEstimator = _

  /** [[LoadEstimator]] to estimate the RAM load of the [[udf]]. */
  private var udfRamEstimator: LoadEstimator = _

  // Try to infer the type classes from the udf.
  locally {
    val parameters = ReflectionUtils.getTypeParameters(udf.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[Out] => this.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", udf)
    }
    parameters.get("Output") match {
      case cls: Class[In] => inputDataQuanta.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", udf)
    }
  }

  /**
    * Set a [[LoadEstimator]] for the CPU load of the UDF.
    *
    * @param udfCpuEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withUdfCpuEstimator(udfCpuEstimator: LoadEstimator) = {
    this.udfCpuEstimator = udfCpuEstimator;
    this
  }

  /**
    * Set a [[LoadEstimator]] for the RAM load of the UDF.
    *
    * @param udfRamEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withUdfRamEstimator(udfRamEstimator: LoadEstimator) = {
    this.udfRamEstimator = udfRamEstimator;
    this
  }

  override protected def build = inputDataQuanta.dataQuanta().mapJava(udf, this.udfCpuEstimator, this.udfRamEstimator)

}


class GlobalReduceDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T],
                                       udf: SerializableBinaryOperator[T])
  extends DataQuantaBuilder[GlobalReduceDataQuantaBuilder[T], T] {

  override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

  /** [[LoadEstimator]] to estimate the CPU load of the [[udf]]. */
  private var udfCpuEstimator: LoadEstimator = _

  /** [[LoadEstimator]] to estimate the RAM load of the [[udf]]. */
  private var udfRamEstimator: LoadEstimator = _

  // Try to infer the type classes from the udf.
  locally {
    val parameters = ReflectionUtils.getTypeParameters(udf.getClass, classOf[SerializableBinaryOperator[_]])
    parameters.get("Type") match {
      case cls: Class[T] => this.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", udf)
    }
  }

  /**
    * Set a [[LoadEstimator]] for the CPU load of the UDF.
    *
    * @param udfCpuEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withUdfCpuEstimator(udfCpuEstimator: LoadEstimator) = {
    this.udfCpuEstimator = udfCpuEstimator;
    this
  }

  /**
    * Set a [[LoadEstimator]] for the RAM load of the UDF.
    *
    * @param udfRamEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withUdfRamEstimator(udfRamEstimator: LoadEstimator) = {
    this.udfRamEstimator = udfRamEstimator;
    this
  }

  override protected def build = inputDataQuanta.dataQuanta().reduceJava(udf, this.udfCpuEstimator, this.udfRamEstimator)

}
