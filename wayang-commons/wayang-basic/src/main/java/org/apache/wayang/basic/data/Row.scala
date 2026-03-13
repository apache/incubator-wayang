package org.apache.wayang.basic.data

/**
 * Represents a single row of data with an associated schema.
 * @param schema The metadata describing the structure of the fields.
 */
case class Row(fields: List[Any], schema: Schema = null) {

  val size: Int = fields.size

  def getFields: List[Any] = fields

  def get(index: Int): Any = fields(index)

  def getSchema: Schema = schema

}

/**
 * Of course a Schema is more complicated than the following Map, what follows is useful to
 * communicate the core of the Schema.
 */
case class Schema (columnNames: Map[String, Class[_]])