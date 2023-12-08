package org.apache.wayang.api.implicits

import scala.reflect.ClassTag

case class DataQuantaAsyncResult[Out: ClassTag](resultString: String, classTag: ClassTag[Out])
