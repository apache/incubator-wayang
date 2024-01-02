package org.apache.wayang.api.serialization.mixins

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty, JsonSubTypes, JsonTypeInfo}
import org.apache.wayang.basic.types.RecordType
import org.apache.wayang.core.types.{BasicDataUnitType, DataUnitGroupType, DataUnitType}

object DataTypeMixIns {


  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[BasicDataUnitType[_]], name = "BasicDataUnitType"),
    new JsonSubTypes.Type(value = classOf[DataUnitGroupType[_]], name = "DataUnitGroupType"),
  ))
  abstract class DataUnitTypeMixIn {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[RecordType], name = "RecordType"),
  ))
  abstract class BasicDataUnitTypeMixIn[T] {
    @JsonCreator
    def this(@JsonProperty("typeClass") typeClass: Class[T]) = {
      this()
    }
  }

  abstract class RecordTypeMixIn {
    @JsonCreator
    def this(@JsonProperty("fieldNames") fieldNames: Array[String]) = {
      this()
    }
  }

  abstract class DataUnitGroupTypeMixIn[T] {
    @JsonCreator
    def this(@JsonProperty("baseType") baseType: DataUnitType[_]) = {
      this()
    }
  }

  abstract class DataSetTypeMixIn[T] {
    @JsonCreator
    def this(@JsonProperty("dataUnitType") dataUnitType: DataUnitType[T]) = {
      this()
    }
  }

}