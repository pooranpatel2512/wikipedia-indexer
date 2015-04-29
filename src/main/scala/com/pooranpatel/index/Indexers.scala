package com.pooranpatel
package index

import org.apache.lucene.index.IndexWriterConfig.OpenMode
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

  val textAnalyzer = new WikipediaTextAnalyzer()

  val indexesDirectory = FSDirectory.open(Paths.get("../index"))

  val indexWriterConfig = new IndexWriterConfig(textAnalyzer)

  indexWriterConfig.setOpenMode(OpenMode.CREATE)

  val indexWriter = new IndexWriter(indexesDirectory, indexWriterConfig)

  val indexer = context.actorOf(FromConfig.props(Props(classOf[Indexer], indexWriter)), "indexer-router")

  override def receive: Receive = {
    case msg @ PageElements(elements) =>
      logger.info("Sending a page-elemets to index router")
      indexer ! msg
    case msg @ Shutdown =>
      indexer ! Stop
      context become shutdownHandler
    case msg @ _ => logger.info("Message = {}", msg)
  }

  def shutdownHandler: Receive = {
    case StopAck =>
      nrIndexers -= 1
      if(nrIndexers == 0) {
        indexWriter.close()
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
