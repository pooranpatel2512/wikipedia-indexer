package com.pooranpatel
package routing

import akka.routing.{BroadcastRoutingLogic, Routee, RoutingLogic, SmallestMailboxRoutingLogic}

import scala.collection.immutable.IndexedSeq
import index.Indexer.Protocol.Stop

class IndexerRoutingLogic extends RoutingLogic {

  val smallestMailboxRoutingLogic = SmallestMailboxRoutingLogic()

  val broadcastRoutingLogic = BroadcastRoutingLogic()

  override def select(message: Any, routees: IndexedSeq[Routee]): Routee =  {
    message match {
      case msg @ Stop => broadcastRoutingLogic.select(msg, routees)
      case _ => smallestMailboxRoutingLogic.select(message, routees)
    }
  }
}
