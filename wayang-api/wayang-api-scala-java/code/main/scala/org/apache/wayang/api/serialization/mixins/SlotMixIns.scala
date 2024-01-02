package org.apache.wayang.api.serialization.mixins

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.{JsonAutoDetect, JsonIdentityInfo, JsonIgnore, JsonSubTypes, JsonTypeInfo, ObjectIdGenerators}
import org.apache.wayang.core.plan.wayangplan.{InputSlot, OutputSlot}

import java.util.List

object SlotMixIns {

  @JsonIdentityInfo(generator = classOf[ObjectIdGenerators.IntSequenceGenerator], property = "@id")
  @JsonAutoDetect(fieldVisibility = Visibility.ANY, getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[InputSlot[_]], name = "InputSlot"),
    new JsonSubTypes.Type(value = classOf[OutputSlot[_]], name = "OutputSlot"),
  ))
  abstract class SlotMixIn[T] {

  }

  abstract class OutputSlotMixIn[T] {
    @JsonIgnore
    private var occupiedSlots: List[InputSlot[T]] = _
  }

}