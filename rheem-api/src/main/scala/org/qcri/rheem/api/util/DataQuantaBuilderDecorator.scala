package org.qcri.rheem.api.util

import de.hpi.isg.profiledb.store.model.Experiment
import org.qcri.rheem.api.{DataQuanta, DataQuantaBuilder, JavaPlanBuilder}
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator
import org.qcri.rheem.core.platform.Platform
import org.qcri.rheem.core.types.DataSetType

/**
  * Utility to extend a [[DataQuantaBuilder]]'s functionality by decoration.
  */
abstract class DataQuantaBuilderDecorator[This <: DataQuantaBuilder[This, Out], Out]
(baseBuilder: DataQuantaBuilder[_, Out])
  extends DataQuantaBuilder[This, Out] {

  /**
    * The type of the [[DataQuanta]] to be built.
    */
  override protected[api] def outputTypeTrap: TypeTrap = baseBuilder.outputTypeTrap

  /**
    * Provide a [[JavaPlanBuilder]] to which this instance is associated.
    */
  override protected[api] implicit def javaPlanBuilder: JavaPlanBuilder = baseBuilder.javaPlanBuilder

  override def withName(name: String): This = {
    baseBuilder.withName(name)
    this.asInstanceOf[This]
  }

  override def withExperiment(experiment: Experiment): This = {
    baseBuilder.withExperiment(experiment)
    this.asInstanceOf[This]
  }

  override def withOutputType(outputType: DataSetType[Out]): This = {
    baseBuilder.withOutputType(outputType)
    this.asInstanceOf[This]
  }

  override def withOutputClass(cls: Class[Out]): This = {
    baseBuilder.withOutputClass(cls)
    this.asInstanceOf[This]
  }

  override def withBroadcast[Sender <: DataQuantaBuilder[_, _]](sender: Sender, broadcastName: String): This = {
    baseBuilder.withBroadcast(sender, broadcastName)
    this.asInstanceOf[This]
  }

  override def withCardinalityEstimator(cardinalityEstimator: CardinalityEstimator): This = {
    baseBuilder.withCardinalityEstimator(cardinalityEstimator)
    this.asInstanceOf[This]
  }

  override def withTargetPlatform(platform: Platform): This = {
    baseBuilder.withTargetPlatform(platform)
    this.asInstanceOf[This]
  }

  override def withUdfJarOf(cls: Class[_]): This = {
    baseBuilder.withUdfJarOf(cls)
    this.asInstanceOf[This]
  }

  override def withUdfJar(path: String): This = {
    baseBuilder.withUdfJar(path)
    this.asInstanceOf[This]
  }

  override protected[api] def dataQuanta(): DataQuanta[Out] = baseBuilder.dataQuanta()
}

