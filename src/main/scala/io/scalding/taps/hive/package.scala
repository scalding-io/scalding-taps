package io.scalding.taps

import cascading.scheme.Scheme
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}
import cascading.scheme.hadoop.TextDelimited
import com.twitter.scalding.HadoopSchemeInstance
import cascading.tuple.Fields

package object hive {

  type HiveSourceScheme = Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]

  
  def osvInputScheme(delimiter: String = "\1", fields: Fields = Fields.ALL,  skipHeader: Boolean = true, quote: String = null) : HiveSourceScheme =
    HadoopSchemeInstance(
      new TextDelimited(
        fields, null, skipHeader, skipHeader, delimiter, false, quote, null, true
      ).asInstanceOf[HiveSourceScheme]
    )

  def csvInputScheme(delimiter: String = ",", fields: Fields = Fields.ALL, skipHeader: Boolean = true, quote: String = null) : HiveSourceScheme =
    HadoopSchemeInstance(
      new TextDelimited(
        fields, null, skipHeader, skipHeader, delimiter, false, quote, null, true
      ).asInstanceOf[HiveSourceScheme]
    )

  def tsvInputScheme(delimiter: String = "\t", fields: Fields, skipHeader: Boolean = true, quote: String = null) : HiveSourceScheme =
    HadoopSchemeInstance(
      new TextDelimited(
        fields, null, skipHeader, skipHeader, delimiter, false, quote, null, true
      ).asInstanceOf[HiveSourceScheme]
    )
}
