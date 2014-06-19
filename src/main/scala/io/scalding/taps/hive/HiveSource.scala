package io.scalding.taps.hive

import com.twitter.scalding._
import cascading.hcatalog.{HCatTap, HCatScheme}
import cascading.tap.{Tap, SinkMode}
import cascading.tuple.Fields
import com.twitter.scalding.Local
import cascading.scheme.{NullScheme, Scheme}
import org.apache.hadoop.mapred._
import java.util.Properties
import java.io.{OutputStream, InputStream}
import org.elasticsearch.hadoop.cascading.lingual.EsFactory.EsScheme
import io.scalding.taps.testsupport.TestTapFactory

case class HiveSource(
                  table: String,
                  sinkMode: SinkMode,
                  db: Option[String] = None,
                  filter: Option[String] = None,
                  hCatScheme: Option[HiveSourceScheme] = None,
                  path: Option[String] = None,
                  sourceFields : Option[Fields] = None
                  ) extends Source {

  val noScheme : Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _] = new NullScheme[JobConf, RecordReader[_, _], OutputCollector[_, _], Any, Any] ()

  def createHCatTap : Tap[_, _, _] =
    new HCatTap(
      db.getOrElse(null),
      table,
      filter.getOrElse(null),
      hCatScheme.getOrElse(null),
      path.getOrElse(null),
      sourceFields.getOrElse(null),
      sinkMode
    ).asInstanceOf[Tap[_, _, _]]


  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    mode match {
      case Local(_) | Hdfs(_, _) => createHCatTap

      case _ =>
        (sourceFields, hCatScheme) match {
          case (_, Some(scheme)) =>
            readOrWrite match {
              case Read =>  TestTapFactory(this, scheme.getSourceFields, SinkMode.REPLACE).createTap(readOrWrite)
              case Write =>  TestTapFactory(this, scheme.getSinkFields, SinkMode.REPLACE).createTap(readOrWrite)
            }

          case (Some(fields), None) => TestTapFactory(this, fields, SinkMode.REPLACE).createTap(readOrWrite)
          case _ => TestTapFactory(this, noScheme, SinkMode.REPLACE).createTap(readOrWrite)
        }
    }
  }

  def withSourceFields(fields: Fields) = copy(sourceFields = Some(fields))
  def withDb(db: String) = copy(db = Some(db))
  def withFilter(filter: String) = copy(filter = Some(filter))
  def withHCatScheme(scheme: HiveSourceScheme) = copy(hCatScheme = Some(scheme))

}
