package io.scalding.taps.elasticsearch.testjob

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import com.twitter.scalding.{Csv, JobTest, TupleConversions}
import io.scalding.taps.elasticsearch.EsSource
import cascading.tuple.{Tuple, Fields}


@RunWith(classOf[JUnitRunner])
class ReadFromESTest extends FlatSpec with Matchers with TupleConversions {
  
  val sampleInputData = List(
    List("name1", "1", "address1"),
    List("name2", "2", "address2"),
    List("name3", "3", "address3")
  )
    
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


}
