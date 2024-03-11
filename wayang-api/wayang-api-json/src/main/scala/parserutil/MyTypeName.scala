package org.apache.wayang.api.json.parserutil

trait MyTypeName[A] {
  def name: String
}

object MyTypeName {

  import scala.reflect.runtime.universe._

  implicit def provide[A](implicit tag: WeakTypeTag[A]): MyTypeName[A] =
    new MyTypeName[A] {
      def name: String = tag.tpe.toString // should work for simple cases
    }
}

