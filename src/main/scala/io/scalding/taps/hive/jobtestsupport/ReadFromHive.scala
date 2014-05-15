package io.scalding.taps.hive.jobtestsupport

import com.twitter.scalding.{Csv, Args, Job}
import io.scalding.taps.hive.HiveSource
import cascading.tap.SinkMode

class ReadFromHive(args: Args) extends Job(args) {

  HiveSource("table", SinkMode.KEEP)
    .read
    .write( Csv("output.csv") )

}
