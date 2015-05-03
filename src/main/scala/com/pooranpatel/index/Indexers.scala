package com.pooranpatel
package index

import com.pooranpatel.index.Indexers.Protocol.ShutdownNow
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import settings.Settings
import java.nio.file.Paths
import akka.actor.{Actor, Props}
import akka.event.{Logging, LoggingAdapter}
import akka.routing.FromConfig
import Indexer.Protocol.{ArticleXMLElements, Stop, StopAck}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.store.FSDirectory
import org.joda.time.DateTime

/**
 * Entry point to all the wikipedia articles indexers
 * @param startTime startTime to indexing process
 * @param indexesDir directory path where to store indexes
 */
class Indexers(startTime: DateTime, indexesDir: String) extends Actor {

  import Indexers.Protocol.Shutdown

  val logger: LoggingAdapter = Logging.getLogger(context.system, this)

  val settings = Settings(context.system)

  var nrIndexers = settings.nrIndexers

  val textAnalyzer = new WikipediaTextAnalyzer()

  val indexesDirectory = FSDirectory.open(Paths.get(indexesDir))

  val indexWriterConfig = new IndexWriterConfig(textAnalyzer)

  indexWriterConfig.setOpenMode(OpenMode.CREATE)

  val indexWriter = new IndexWriter(indexesDirectory, indexWriterConfig)

  val indexer = context.actorOf(FromConfig.props(Props(classOf[Indexer], indexWriter)), "indexer-router")

  override def postStop(): Unit = {
    indexWriter.close()
    logger.info("Closing an index writer")
    super.postStop()
  }

  override def receive: Receive = {
    case msg @ ArticleXMLElements(elements) =>
      indexer ! msg
    case msg @ Shutdown =>
      indexer ! Stop
      context become shutdownHandler
    case ShutdownNow =>
      context.system.shutdown()
    case msg @ _ => logger.warning("Message = {}", msg)
  }

  /**
   * Shutdown handler, which waits until all indexers are stopped and then shutdown an actor system
   */
  def shutdownHandler: Receive = {
    case StopAck =>
      nrIndexers -= 1
      if(nrIndexers == 0) {
        logger.info("Time taken for indexing in seconds = {}", (new DateTime().getMillis - startTime.getMillis) / 1000)
        context.system.shutdown()
      }

    case msg @ _ => logger.warning("Unknown msg = {} received in shutdown handler", msg)
  }
}

/**
 * Companion object of [[Indexers]]
 */
object Indexers {

  /**
   * Message communication protocol for actor [[Indexers]]
   */
  object Protocol {

    /**
     * Shutdown message to shutdown actor system
     */
    case object Shutdown

    /**
     * Shutdown message to indicate a abrupt shutdown from a user
     */
    case object ShutdownNow

  }
}
