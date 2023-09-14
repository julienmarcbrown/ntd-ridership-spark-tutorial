package com.ganzekarte.examples.etl.steps.step_5

import spray.json._



object FieldTransformationDefinitions extends DefaultJsonProtocol {
  implicit val fieldTransformationDefinitionFormat: RootJsonFormat[FieldTransformationDefinition] = jsonFormat9(FieldTransformationDefinition)

  val FieldDefinitions: List[FieldTransformationDefinition] = {
    val source = scala.io.Source.fromFile("./resources/ridership_master.json")
    val content = source.mkString
    val parsed = content.parseJson.convertTo[List[FieldTransformationDefinition]]
    parsed
  }

  val ChecksumFields: Seq[FieldTransformationDefinition] = FieldDefinitions
    .filter(_.isChecksumable)

  case class FieldTransformationDefinition(
                                            excelTabName: String,
                                            sanitizedName: String,
                                            dataType: String,
                                            isNullable: Boolean,
                                            isEncodeable: Boolean,
                                            isChecksumable: Boolean,
                                            isShared: Boolean,
                                            isLabel: Boolean,
                                            isFeature: Boolean,
                                          )
}
