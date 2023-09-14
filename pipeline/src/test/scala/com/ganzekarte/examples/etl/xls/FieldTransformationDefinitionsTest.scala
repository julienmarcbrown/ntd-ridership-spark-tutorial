package com.ganzekarte.examples.etl.xls


import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class FieldTransformationDefinitionsTest extends AnyFlatSpec with Matchers {

  "FieldTransformationDefinitions" should "parse ridership_master.json correctly" in {
    val definitions = FieldTransformationDefinitions.FieldDefinitions
    definitions should not be empty
  }

  it should "filter checksum fields correctly" in {
    val checksumFields = FieldTransformationDefinitions.ChecksumFields
    checksumFields.foreach(field => field.isChecksumable should be(true))
  }

}

