import com.pooranpatel.index.{Indexers, Indexer}
import Indexer.Protocol.ArticleXMLElements
import akka.actor.{ActorRef, Props, ActorSystem}
import org.joda.time.DateTime
import scala.collection.{immutable, mutable}
import scala.io.Source
import scala.xml.pull._
import com.pooranpatel.index.Indexers.Protocol.{ShutdownNow, Shutdown}


/**
 * Starting point of the wikipedia articles indexing App
 */
object MainApp extends App {

  if(args.length != 2) {
    println("Usage: java -jar WikipediaIndexer-0.1.0.jar <ARTICLES_FILE> <INDEX_DIR>")
    println("<ARTICLES_FILE> = path to the wikipedia articles file")
    println("<INDEX_DIR> = directory to store indexes")
  } else {
    val system: ActorSystem = ActorSystem("hello-akka")
    val indexer = system.actorOf(Props(classOf[Indexers], new DateTime(), args(1)), "indexers")

    sys addShutdownHook {
      println("Shutting down an app")
      indexer ! ShutdownNow
      Thread.sleep(1500)
      println("Done shutting down an app")
    }

    readAndIndexArticles(args(0), indexer)
    indexer ! Shutdown
  }


  /**
   * This method reads a given wikipedia articles file and give this articles to indexer to index
   * @param pathToArticlesFile path to to the wikipedia articles file
   * @param indexer wikipedia article indexer
   */
  private def readAndIndexArticles(pathToArticlesFile: String, indexer: ActorRef) = {
    val reader: XMLEventReader = new XMLEventReader(Source.fromFile(pathToArticlesFile))
    var counter = 0
    var insidePage = false
    var articlesXMLElements: mutable.Queue[XMLEvent] = mutable.Queue.empty

    reader.foreach { event =>
      event match {
        case EvElemStart(_, "page", _, _) =>
          insidePage = true
          articlesXMLElements += event
        case EvElemEnd(_, "page") =>
          articlesXMLElements += event
          insidePage = false
          indexer ! ArticleXMLElements(articlesXMLElements.toList)
          counter += 1
          articlesXMLElements = mutable.Queue.empty
        case EvElemStart(_, tag, _, _) =>
          if (insidePage) {
            articlesXMLElements += event
          }
        case EvElemEnd(_, tag) =>
          if (insidePage) {
            articlesXMLElements += event
          }
        case EvText(t) =>
          if (insidePage) {
            articlesXMLElements += event
          }
        case _ => // ignore
      }
    }
  }
}
