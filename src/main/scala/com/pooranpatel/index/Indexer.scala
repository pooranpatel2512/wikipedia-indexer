package com.pooranpatel.index

import java.io.StringReader

import akka.actor.Actor
import akka.event.{Logging, LoggingAdapter}
import com.pooranpatel.index.Indexer.model.Article
import org.apache.lucene.document.{Document, Field, TextField}
import org.apache.lucene.index.IndexWriter

import scala.xml.pull._


/**
 * Actor which indexes a wikipedia article
 * @param indexWriter lucene index writer for index directory
 */
class Indexer(indexWriter: IndexWriter) extends Actor {

  import Indexer.Protocol.{ArticleXMLElements, Stop, StopAck}

  val logger: LoggingAdapter = Logging.getLogger(context.system, this)

  private var indexedArticles: Int = 0

  override def receive: Receive = {
    case ArticleXMLElements(elements) =>
      val page = parseArticleXMLElements(elements)
      page.foreach(page => indexArticle(page))
    case Stop =>
      sender() ! StopAck
      context stop self
    case _ =>
      logger.warning("Unknown message is received at indexer")
  }

  /**
   * Method to index wikipedia article and store them to indexes directory
   * @param article wikipedia article
   */
  private def indexArticle(article: Article): Unit = {
    val doc = new Document()
    doc.add(new TextField("title", article.title, Field.Store.YES))
    doc.add(new TextField("text", new StringReader(article.text)))
    article.contributor.foreach { contributor =>
      doc.add(new TextField("contributor", contributor, Field.Store.YES))
    }
    indexWriter.addDocument(doc)
    indexedArticles += 1
    if(indexedArticles == 500) {
      logger.info("500 wikipedia articles indexed")
      indexedArticles = 0
    }
  }

  /**
   * Extract title, contributor and text from wikipedia article XML elements
   * @param articleXMLElements XML elements of wikipedia article
   * @return Option of wikipedia article
   */
  private def parseArticleXMLElements(articleXMLElements: List[XMLEvent]): Option[Article] = {

    var pageTitle: Option[String] = None
    var pageContributor: Option[String] = None
    val pageText: StringBuilder = new StringBuilder()

    val iter = articleXMLElements.iterator
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
            case EvElemStart(_, "username", _, _) =>
              iter.next() match {
                case EvText(contributor) => pageContributor = Some(contributor)
                case e @ _ => logger.warning("Some unknown element after username")
              }
            case e @ _ =>
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
    } yield Article(title, pageContributor, pageText.toString())
  }
}

/**
 * Companion object of [[Indexer]]
 */
object Indexer {

  object model {

    /**
     * Wikipedia article
     * @param title title
     * @param contributor contributor of an article
     * @param text raw text of an article
     */
    case class Article(title: String, contributor: Option[String], text: String)
  }

  /**
   * Message communication protocol for actor [[Indexer]]
   */
  object Protocol {

    /**
     * XML elements of a wikipedia article
     * @param elements XML elements
     */
    case class ArticleXMLElements(elements: List[XMLEvent])

    /**
     * Stop message for stopping an actor
     */
    case object Stop

    /**
     * Acknowledgment message for [[Stop]] message
     */
    case object StopAck
  }
}
