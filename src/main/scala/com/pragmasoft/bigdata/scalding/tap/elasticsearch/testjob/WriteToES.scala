package com.pragmasoft.bigdata.scalding.tap.elasticsearch.testjob

import com.twitter.scalding.{Csv, IterableSource, Job, Args}
import com.pragmasoft.bigdata.scalding.tap.elasticsearch.EsSource

object WriteToES {
  val testIndex = "test"
  val ageType = "personAge"
  val addressType = "personAddress"
  val fullType = "personFull"
  
  val ageResource = s"$testIndex/$ageType"
  val addressResource = s"$testIndex/$addressType"
  val fullResource = s"$testIndex/$fullType"

  val inputFields = ('name, 'age, 'address, 'useless)
}

import WriteToES._
class WriteToES(args: Args) extends Job(args) {
  val source = Csv(args("input"), ",", inputFields).read

  val sinkAge = EsSource(ageResource).withFields(('name, 'age)).withMappingId(mappingId = "name")
  val sinkAddress = EsSource(addressResource).withFields(('name, 'address)).withMappingId(mappingId = "name")
  val sinkFull = EsSource(fullResource).withFields(inputFields).withMappingId(mappingId = "name")

  source.write(sinkAge)
  source.write(sinkAddress)
  source.write(sinkFull)
}
