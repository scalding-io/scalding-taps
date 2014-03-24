package com.pragmasoft.bigdata.scalding.tap.elasticsearch

import com.twitter.scalding.{Csv, Job, Args}

class ReadFromES(args: Args) extends Job(args) {

   val fields = ('name, 'age, 'address)

   val source = EsSource("test/personFull").withHost("localhost").withPort(9200).withFields(fields).read

   source.write( Csv("output.csv") )
 }
