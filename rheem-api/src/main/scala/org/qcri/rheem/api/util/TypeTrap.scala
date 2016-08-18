package org.qcri.rheem.api.util

import org.qcri.rheem.core.types.DataSetType

/**
  * This class waits for a [[org.qcri.rheem.core.types.DataSetType]] to be set and verifies that there are no
  * two different sets.
  */
class TypeTrap {

  /**
    * Stores the [[DataSetType]].
    */
  private var _dataSetType: DataSetType[_] = _

  /**
    * Set the [[DataSetType]] for this instance.
    *
    * @throws IllegalArgumentException if a different [[DataSetType]] has been set before
    * @param dst the [[DataSetType]]
    */
  def dataSetType_=(dst: DataSetType[_]): Unit = {
    _dataSetType match {
      case null => _dataSetType = dst
      case `dst` =>
      case other => throw new IllegalArgumentException(s"Conflicting types ${_dataSetType} and ${dst}.")
    }
  }

  /**
    * Return the previously set [[DataSetType]].
    *
    * @return the previously set [[DataSetType]] or, if none has been set, [[DataSetType.none()]]
    */
  def dataSetType = _dataSetType match {
    case null => DataSetType.none()
    case other => other
  }

  /**
    * Return the [[Class]] of the previously set [[DataSetType]].
    *
    * @return the [[Class]] of the previously set [[DataSetType]] or, if none has been set, that of [[DataSetType.none()]]
    */
  def typeClass = dataSetType.getDataUnitType.toBasicDataUnitType.getTypeClass
}
