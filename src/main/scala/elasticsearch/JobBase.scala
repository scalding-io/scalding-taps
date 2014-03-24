package elasticsearch

import com.twitter.scalding._

class JobBase(args: Args) extends Job(args) {

  // Overide JobConfig
  override def config : Map[AnyRef,AnyRef] = {
    super.config  ++ Map(ConfigurationOptions.ES_WRITE_OPERATION -> "index") ++ Map (ConfigurationOptions.ES_MAPPING_ID -> "number")
  }

}