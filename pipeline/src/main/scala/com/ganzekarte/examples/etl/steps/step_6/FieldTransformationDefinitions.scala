package com.ganzekarte.examples.etl.steps.step_6

import spray.json._

import scala.io.Source

/**
 * Object FieldTransformationDefinitions provides utilities for working with field transformation definitions.
 * It supports deserializing from a JSON configuration, and has predefined lists to categorize fields based on their attributes.
 */
object FieldTransformationDefinitions extends DefaultJsonProtocol {

  // Defines the implicit format for deserializing the FieldTransformationDefinition object.
  implicit val fieldTransformationDefinitionFormat: RootJsonFormat[FieldTransformationDefinition] = jsonFormat9(FieldTransformationDefinition)

  /**
   * Reads the field transformation definitions from a JSON file located in the resources directory.
   * The JSON structure should correspond to the FieldTransformationDefinition case class.
   */
  val FieldDefinitions: List[FieldTransformationDefinition] = {
    val inputStream = getClass.getResourceAsStream("/field_definitions.json")
    val source = Source.fromInputStream(inputStream)

    try {
      val content = source.mkString
      content.parseJson.convertTo[List[FieldTransformationDefinition]]
    } catch {
      case e: Exception =>
        throw new RuntimeException("Failed to parse field_definitions.json", e)
    } finally {
      source.close()
    }
  }

  /**
   * Filters the FieldDefinitions to only retain those fields that can be used for generating checksums.
   */
  val ChecksumFields: Seq[FieldTransformationDefinition] = FieldDefinitions
    .filter(_.isChecksumable)

  /**
   * Case class representing the attributes of a field in the transformation process.
   *
   * @param excelTabName   The name of the field as it appears in the Excel tab.
   * @param sanitizedName  The sanitized version of the field name, typically used in column naming after transformation.
   * @param dataType       The target data type of the field after transformation.
   * @param isNullable     Flag indicating if the field can have null values.
   * @param isEncodeable   Flag indicating if the field values should be encoded.
   * @param isChecksumable Flag indicating if the field should be considered while generating checksums.
   * @param isShared       Flag indicating if the field is shared across multiple tabs or datasets.
   * @param isLabel        Flag indicating if the field is a label in machine learning terms.
   * @param isFeature      Flag indicating if the field is a feature in machine learning terms.
   */
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
