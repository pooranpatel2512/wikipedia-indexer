package com.pooranpatel.index

import java.io.StringReader

import akka.actor.Actor
import akka.event.{Logging, LoggingAdapter}
import com.pooranpatel.index.Indexer.model.Page
import org.apache.lucene.document.{Document, Field, TextField}
import org.apache.lucene.index.IndexWriter

import scala.collection.mutable
import scala.xml.pull._



class Indexer(iw: IndexWriter) extends Actor {

  import Indexer.Protocol.{PageElements, Stop, StopAck}

  val logger: LoggingAdapter = Logging.getLogger(context.system, this)

  override def postStop(): Unit = {
    logger.warning("actor is shutting down")
    super.postStop()
  }

  override def receive: Receive = {
    case PageElements(elements) =>
      val page = parsePageElemets(elements)
      page.foreach(page => indexPage(page))
    case Stop =>
      logger.info("Stop msg is received")
      sender() ! StopAck
      context stop self
    case _ =>
      logger.warning("Unknown message is received at indexer")
  }

  private def indexPage(page: Page): Unit = {
    logger.info("Indexing page = {}", page.title)
    val doc = new Document()
    doc.add(new TextField("title", page.title, Field.Store.YES))
    doc.add(new TextField("text", new StringReader(page.text)))
    page.contributor.map { contributor =>
      doc.add(new TextField("contributor", contributor, Field.Store.YES))
    }
    iw.addDocument(doc)
  }

  private def parsePageElemets(elements: mutable.Queue[XMLEvent]): Option[Page] = {

    var pageTitle: Option[String] = None
    var pageContributor: Option[String] = None
    val pageText: StringBuilder = new StringBuilder()

    val iter = elements.iterator
    while(iter.hasNext) {
      val e = iter.next()
      e match {
        case EvElemStart(pre, "title", attrs, _) =>
          iter.next() match {
            case EvText(title) => pageTitle = Some(title)
            case _ => logger.warning("Some unknown element after title")
          }
        case EvElemStart(pre, "contributor", attrs, _) =>
          iter.next()
          iter.next() match {
            case EvElemStart(pre, "username", attrs, _) =>
              iter.next() match {
                case EvText(contributor) => pageContributor = Some(contributor)
                case e @ _ => logger.warning("Some unknown element after username = {}", e)
              }
            case e @ _ => logger.warning("Some unknown element after contributor = {} ", e)
          }
        case EvElemStart(pre, "text", attrs, _) =>
          iter.next() match {
            case EvText(text) => pageText.append(text)
            case _ => logger.warning("Some unknown element after text")
          }
        case EvText("ref") =>
          iter.next() match {
            case EvText(text) => pageText.append(text)
            case _ => logger.warning("Some unknown element after ref")
          }
        case _ =>
      }
    }
    for {
      title <- pageTitle
    } yield Page(title, pageContributor, pageText.toString())
  }
}

object Indexer {

  object model {
    case class Page(title: String, contributor: Option[String], text: String)
  }

  object Protocol {
    case class PageElements(queue: mutable.Queue[XMLEvent])
    case object Stop
    case object StopAck
  }
}
