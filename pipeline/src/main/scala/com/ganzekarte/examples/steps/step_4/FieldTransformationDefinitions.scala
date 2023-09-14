package com.ganzekarte.examples.steps.step_4

import spray.json._



object FieldTransformationDefinitions extends DefaultJsonProtocol {
  implicit val fieldTransformationDefinitionFormat: RootJsonFormat[FieldTransformationDefinition] = jsonFormat4(FieldTransformationDefinition)

  val FieldDefinitions: List[FieldTransformationDefinition] = {
    val source = scala.io.Source.fromFile("/Users/julien/IdeaProjects/transit-karte/pipeline/src/main/resources/ridership_master.json")
    val content = source.mkString
    val parsed = content.parseJson.convertTo[List[FieldTransformationDefinition]]
    parsed
  }

  case class FieldTransformationDefinition(
                                            excelTabName: String,
                                            sanitizedName: String,
                                            dataType: String,
                                            isNullable: Boolean
                                          )
}
