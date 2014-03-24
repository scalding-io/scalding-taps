package com.pragmasoft.bigdata.scalding.tap.elasticsearch

import com.twitter.scalding._
import cascading.tap.Tap
import com.twitter.scalding.Local
import org.elasticsearch.hadoop.cascading.EsTap
import cascading.tuple.Fields
import java.util.Properties
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import ConfigurationOptions._


object EsSource {
  sealed trait EsWriteMode {
    def writeOperationParam: String
  }

  object Index extends EsWriteMode { override val writeOperationParam = ES_OPERATION_INDEX }

  object Create extends EsWriteMode { override val writeOperationParam = ES_OPERATION_CREATE }

  object Update extends EsWriteMode { override val writeOperationParam = ES_OPERATION_UPDATE }

  object Delete extends EsWriteMode { override val writeOperationParam = ES_OPERATION_DELETE }

}

import EsSource._

case class EsSource(
                esResource: String,
                esHost: Option[String] = None,
                esPort: Option[Int] = None,
                query: Option[String] = None,
                fields: Option[Fields] = None,
                settings: Option[Properties] = None
         ) extends Source {


  private def overrideSettings(overrides: Properties => Unit): Option[Properties] = {
    val toOverride = settings.getOrElse(new Properties())

    overrides(toOverride)

    Some(toOverride)
  }

  def withPort(port: Int): EsSource = copy(esPort = Some(port))

  def withHost(host: String): EsSource = copy(esHost = Some(host))

  def withQuery(query: String): EsSource = copy(query = Some(query))

  def withFields(fields: Fields): EsSource = copy(fields = Some(fields))

  def withSettings(settings: Properties): EsSource = copy(settings = Some(settings))

  def withWriteMode(writeMode: EsWriteMode): EsSource = copy(settings = overrideSettings {
    _.setProperty(ES_WRITE_OPERATION, writeMode.writeOperationParam)
  })

  def withMappingId(mappingId: String, mappingIdExtractorClassName: Option[String] = None): EsSource =
    copy(
      settings = overrideSettings {
        props =>
          props.setProperty(ES_MAPPING_ID, mappingId)
          mappingIdExtractorClassName.foreach {
            extractorClass => props.setProperty(ES_MAPPING_ID_EXTRACTOR_CLASS, extractorClass)
          }
      }
    )


  def createEsTap: Tap[_, _, _] =
    new EsTap(esHost.getOrElse(null), esPort.getOrElse(-1), esResource, query.getOrElse(null), fields.getOrElse(null), settings.getOrElse(null))

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    mode match {
      case Local(_) | Hdfs(_, _) => createEsTap

      case _ => super.createTap(readOrWrite)(mode)
    }
  }
}
