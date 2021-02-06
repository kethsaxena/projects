package com.surescripts.edm.schema

import net.liftweb.json._

case class TableSchema(table: Table, extract: Extract, fields: List[Field])

case class Table(dbName: String, name: String, partitionName: String)

case class Extract(sourceDb: String, sourceTable: String, sourceSchema: String)

case class Field(name: String, columnType: List[String], logicalType: Option[String])

object TableSchema {
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /**
   * Parse a json schema file to a TableSchema object
   * @param json - json representing the avro schema file with ingestion extensions
   * @return TableSchema that maps to the json
   */
  def parseSchema(json: String) : TableSchema = {
    val parsed = parse(json) transformField {
      case JField("type", v) => JField("columnType", v)
      case JField("logicaltype", v) => JField("logicalType", v)
      case JField(fieldName, v) => JField(toCamel(fieldName), v)

    }
    parsed.extract[TableSchema]
  }

  private def toCamel(s: String): String = {
    val split = s.split("_")
    val tail = split.tail.map { x => x.head.toUpper + x.tail }
    split.head + tail.mkString
  }

}


