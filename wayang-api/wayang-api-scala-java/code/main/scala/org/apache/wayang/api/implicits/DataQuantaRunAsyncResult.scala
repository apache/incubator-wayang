package org.apache.wayang.api.implicits

import scala.reflect.ClassTag

case class DataQuantaRunAsyncResult[Out: ClassTag](tempFileOut: String, classTag: ClassTag[Out])
