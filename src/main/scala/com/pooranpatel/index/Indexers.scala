package com.pooranpatel
package index

import settings.Settings
import java.nio.file.Paths
import akka.actor.{Actor, Props}
import akka.event.{Logging, LoggingAdapter}
import akka.routing.FromConfig
import com.pooranpatel.index.Indexer.Protocol.{PageElements, Stop, StopAck}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory
import org.joda.time.DateTime

class Indexers(startTime: DateTime) extends Actor {

  import Indexers.Protocol.Shutdown

  val logger: LoggingAdapter = Logging.getLogger(context.system, this)

  val settings = Settings(context.system)

  var nrIndexers = settings.nrIndexers

  lazy val analyzer = new StandardAnalyzer()

  lazy val dir = FSDirectory.open(Paths.get("./index"))

  lazy val iwc = new IndexWriterConfig(analyzer)

  lazy val iw = new IndexWriter(dir, iwc)

  val indexer = context.actorOf(FromConfig.props(Props(classOf[Indexer], iw)), "indexer-router")

  override def receive: Receive = {
    case msg @ PageElements(elements) => indexer ! msg
    case msg @ Shutdown =>
      indexer ! Stop
      context become shutdownHandler
    case msg @ _ => logger.info("Message = {}", msg)
  }

  def shutdownHandler: Receive = {
    case StopAck =>
      nrIndexers -= 1
      if(nrIndexers == 0) {
        iw.close()
        logger.info("Time taken for indexing in seconds = {}", (new DateTime().getMillis - startTime.getMillis) / 1000)
        context.system.shutdown()
      }

    case msg @ _ => logger.warning("Unknown msg = {} received in shutdown handler", msg)
  }
}

object Indexers {
  object Protocol {
    case object Shutdown
  }
}
