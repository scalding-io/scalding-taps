package io.scalding.taps.elasticsearch

import org.scalatest.{FlatSpec, Matchers}
import com.twitter.scalding.{FieldConversions, JobTest, TupleConversions}
import cascading.tuple.{Fields, Tuple}
import java.util.Properties
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.hadoop.cascading.CascadingFieldExtractor
import io.scalding.taps.elasticsearch.jobtestsupport.WriteToES._
import com.twitter.scalding.Csv
import scala.Some
import io.scalding.taps.elasticsearch.jobtestsupport.{ReadFromES, WriteToES}
import io.scalding.taps.hive.jobtestsupport.ReadFromHive
import io.scalding.taps.hive.HiveSource
import cascading.tap.SinkMode

class JobTestSupportSpec extends FlatSpec with Matchers with TupleConversions with FieldConversions {

  val sampleInputData = List(
    List("name1", "1", "address1", "useless1"),
    List("name2", "2", "address2", "useless2"),
    List("name3", "3", "address3", "useless3")
  )

  val inputFileName = "input.csv"

  val esProperties = new Properties()
  esProperties.setProperty(ES_MAPPING_ID_EXTRACTOR_CLASS, classOf[CascadingFieldExtractor].getName)
  esProperties.setProperty(ES_MAPPING_ID, "name")


  behavior of "EsTap"

  JobTest(classOf[WriteToES].getName)
    .arg("local", "")
    .arg("input", inputFileName)
    .source[Tuple](Csv(inputFileName, fields = inputFields),
      sampleInputData.map(l => new Tuple(l: _*)))
    .sink[Tuple](EsSource(WriteToES.fullResource,None,None,None,Some(inputFields),Some(esProperties))){
      outputBuffer => {
        it should "support test mode" in {
          assert(outputBuffer.size == sampleInputData.size)
        }
      }
    }
    .sink[Tuple](EsSource(WriteToES.addressResource,None,None,None,Some(new Fields("name", "address")),Some(esProperties))){
      outputBuffer =>
    }
    .sink[Tuple](EsSource(WriteToES.ageResource,None,None,None,Some(new Fields("name", "age")),Some(esProperties))){
      outputBuffer =>
    }
    .run
    .finish


  behavior of "ReadFromES"

  JobTest(classOf[ReadFromES].getName)
    .arg("local", "")
    .source[Tuple](EsSource("test/personFull",Some("localhost"),Some(9200),None,Some(new Fields("name", "age", "address")),None),
      sampleInputData.map(l => new Tuple(l: _*)))
    .sink[Tuple](Csv("output.csv")) {
    outputBuffer => {
      it should "read from ES and write into HDFS" in {
        outputBuffer.toList should equal(sampleInputData.map ((fields: List[String]) => new Tuple(fields:_*) ))
      }
    }
  }
  .run
  .finish


  behavior of "HiveTap"

  JobTest(classOf[ReadFromHive].getName)
    .source[Tuple](HiveSource("table", SinkMode.KEEP),
      sampleInputData.map(l => new Tuple(l: _*)))
    .sink[Tuple](Csv("output.csv")) {
      outputBuffer => {
        it should "support test mode" in {
          assert(outputBuffer.size == sampleInputData.size)
        }
      }
    }
    .run
    .finish

}
