package io.scalding.taps.elasticsearch

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.github.tlrx.elasticsearch.test.EsSetup
import com.github.tlrx.elasticsearch.test.EsSetup._
import com.twitter.scalding.Tool
import io.scalding.taps.elasticsearch.jobtestsupport.WriteToES
import java.io.{FileWriter, BufferedWriter, File}
import org.elasticsearch.action.get.{GetResponse, GetRequest}
import WriteToES._
import scala.concurrent.duration._
import scala.util.Try
import scala.collection.JavaConversions._

class EsSourceSpec extends FlatSpec with Matchers {

  behavior of "EsSource used as Tap"

  it should "Allow to write entries to a resources" in new EmeddedEsTest(
    input = List(
      "user1,31,Address 1,useless field 1",
      "user2,32,Address 2,useless field 2",
      "user3,33,Address 3,useless field 3"),
    
    test = {
      embeddedEs =>
          withinTimeout(30 seconds) {
            embeddedEs.getDocumentSource(testIndex, fullType, "user1") should equal( Map( "address" -> "Address 1", "age" -> "31", "name" -> "user1", "useless" -> "useless field 1" ) )
            embeddedEs.getDocumentSource(testIndex, addressType, "user1") should equal( Map( "address" -> "Address 1", "name" -> "user1" ) )
            embeddedEs.getDocumentSource(testIndex, ageType, "user1") should equal( Map( "age" -> "31", "name" -> "user1" ) )
          }
      }
  )

  // cannot run in hdfs mode with embedded ES
  ignore should "Allow to write entries to a resources in hdfs mode" in new EmeddedEsTest(
    input = List(
      "user1,31,Address 1,useless field 1",
      "user2,32,Address 2,useless field 2",
      "user3,33,Address 3,useless field 3"),

    test = {
      embeddedEs =>
        withinTimeout(30 seconds) {
          embeddedEs.getDocumentSource(testIndex, fullType, "user1") should equal( Map( "address" -> "Address 1", "age" -> "31", "name" -> "user1", "useless" -> "useless field 1" ) )
          embeddedEs.getDocumentSource(testIndex, addressType, "user1") should equal( Map( "address" -> "Address 1", "name" -> "user1" ) )
          embeddedEs.getDocumentSource(testIndex, ageType, "user1") should equal( Map( "age" -> "31", "name" -> "user1" ) )
        }
    },

    runMode = "--hdfs"
  )

  def withinTimeout(timeout: FiniteDuration)(assertion: => Unit): Unit = {
    val until = System.currentTimeMillis + timeout.toMillis
    var lastTry: Try[Unit] = Try {
      assertion
    }
    do {
      if (lastTry.isFailure) {
        Thread.sleep(timeout.toMillis / 10)
        lastTry = Try {
          assertion
        }
      }
    } while ((System.currentTimeMillis <= until) && lastTry.isFailure)

    lastTry.get
  }
}

class EmeddedEsTest(input: List[String], test: EmeddedEsTest => Unit, runMode: String = "--local") {
  def writeContent(contentFile: File, content: String*): Unit = {
    val writer = new BufferedWriter(new FileWriter(contentFile))
    content foreach {
      line =>
        writer.write(line)
        writer.newLine()
    }
    writer.flush()
    writer.close()
  }

  def getDocument(index: String, `type`: String, id: String): GetResponse =
    esSetup.client().get(new GetRequest(index, `type`, id)).actionGet(2.seconds.toMillis)

  def getDocumentSource(index: String, `type`: String, id: String): Map[String, AnyRef] =
    esSetup.client().get(new GetRequest(index, `type`, id)).actionGet(2.seconds.toMillis).getSource.toMap

  def runWriteJob(contentFile: File, runMode: String = "--local"): Unit =
    Tool.main(Array(classOf[WriteToES].getName, runMode, "--input", contentFile.getAbsolutePath))

  def writeDocument(index: String, `type`: String, id: String, content: Map[String, AnyRef]) : Unit =
    esSetup.client().prepareUpdate().setId(id).setType(`type`).setIndex(index).setDoc(content).execute()

  val esSetup = new EsSetup()

  try {
    esSetup.execute(deleteAll)

    val contentFile = File.createTempFile("esSource", "csv")
    contentFile.deleteOnExit()

    writeContent(contentFile, input: _*)

    runWriteJob(contentFile, runMode)

    test(this)

  } finally {
    esSetup.terminate()
  }
}
