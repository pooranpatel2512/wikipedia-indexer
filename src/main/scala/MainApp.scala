import com.pooranpatel.index.{Indexers, Indexer}
import Indexer.Protocol.PageElements
import akka.actor.{Props, ActorSystem}
import com.pooranpatel.settings.Settings
import org.joda.time.DateTime
import scala.collection.mutable
import scala.io.Source
import scala.xml.pull._
import scala.util.control._
import Indexers.Protocol.Shutdown


object MainApp extends App {

  val system: ActorSystem = ActorSystem("hello-akka")
  val settings = Settings(system)
  val indexers = system.actorOf(Props(classOf[Indexers], new DateTime()), "indexers")

  val reader: XMLEventReader = new XMLEventReader(Source.fromFile("/Users/pooranpatel/Downloads/data/enwiki-latest-pages-articles.xml"))
  val loop = new Breaks
  var counter = 0
  var insidePage = false
  var pageElements: mutable.Queue[XMLEvent] = mutable.Queue.empty
  loop.breakable {
    reader.foreach { event =>
      event match {
        case EvElemStart(_, "page", _, _) =>
          insidePage = true
          pageElements += event
        case EvElemEnd(_, "page") =>
          pageElements += event
          insidePage = false
          indexers ! PageElements(pageElements)
          counter += 1
          pageElements = mutable.Queue.empty
        case EvElemStart(_, tag, _, _) => {
          if (insidePage) {
            pageElements += event
          }
        }
        case EvElemEnd(_, tag) => {
          if (insidePage) {
            pageElements += event
          }
        }
        case EvText(t) => {
          if (insidePage) {
            pageElements += event
          }
        }
        case _ => // ignore
      }
      if(counter == 200) {
        loop.break()
      }
    }
  }
  indexers ! Shutdown
}
