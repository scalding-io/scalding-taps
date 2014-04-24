package io.scalding.taps.elasticsearch.testjob

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers, FunSpec}
import com.twitter.scalding.{Csv, JobTest, TupleConversions}
import cascading.tuple.{Fields, Tuple}
import io.scalding.taps.elasticsearch.EsSource
import java.util.Properties
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import scala.Some
import org.elasticsearch.hadoop.cascading.CascadingFieldExtractor


@RunWith(classOf[JUnitRunner])
class WriteToESTest extends FlatSpec with Matchers with TupleConversions {

  val sampleInputData = List(
    List("name1", "1", "address1", "useless1"),
    List("name2", "2", "address2", "useless2"),
    List("name3", "3", "address3", "useless3")
  )

  val inputFileName = "input.csv"

  val esProperties = new Properties()
  esProperties.setProperty(ES_MAPPING_ID_EXTRACTOR_CLASS, classOf[CascadingFieldExtractor].getName)
  esProperties.setProperty(ES_MAPPING_ID, "name")


  behavior of "WriteToES"

  JobTest(classOf[WriteToES].getName)
    .arg("local", "")
    .arg("input", inputFileName)
    .source[Tuple](Csv(inputFileName, fields = new Fields("name", "age", "address", "useless")),
      sampleInputData.map(l => new Tuple(l: _*)))
    .sink[Tuple](EsSource(WriteToES.ageResource,None,None,None,Some(new Fields("name", "age")),Some(esProperties))){
      outputBuffer => {
        it should "write to ES people's age" in {
          outputBuffer.toList should equal(sampleInputData.map {fields: List[String] => new Tuple(fields(0), fields(1)) } )
        }
      }
    }
    .sink[Tuple](EsSource(WriteToES.addressResource,None,None,None,Some(new Fields("name", "address")),Some(esProperties))){
      outputBuffer => {
        it should "write to ES people's address" in {
          assert(outputBuffer.size == sampleInputData.size)
        }
      }
    }
    .sink[Tuple](EsSource(WriteToES.fullResource,None,None,None,Some(new Fields("name", "age", "address", "useless")),Some(esProperties))){
      outputBuffer => {
        it should "write to ES all people's information" in {
          assert(outputBuffer.size == sampleInputData.size)
        }
      }
    }
    .run
    .finish


}
