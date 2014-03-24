package com.pragmasoft.bigdata.scalding.tap.elasticsearch

import com.twitter.scalding.{IterableSource, Job, Args}

class WriteToES(args: Args) extends Job(args) {

  val inputFields = ('name, 'age, 'address, 'useless)

  val source = new IterableSource(
    List(
      ("Stefano", 40, "Regency Lodge CHANGED", "useless"),
      ("Michele", 35, "Regency Lodge CHANGED", "useless"),
      ("Somebody Else", 40, "Somewhere Else CHANGED", "useless")
    ),
    inputFields
  ).read

  val sink1 = EsSource("test/person").withFields(('name, 'age)).withMappingId(mappingId = "name")
  val sink2 = EsSource("test/person2").withFields(('name, 'address)).withMappingId(mappingId = "name")
  val sink3 = EsSource("test/personFull").withFields(inputFields).withMappingId(mappingId = "name")

  source.write(sink1)
  source.write(sink2)
  source.write(sink3)
}
