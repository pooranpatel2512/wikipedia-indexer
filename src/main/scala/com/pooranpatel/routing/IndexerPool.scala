package com.pooranpatel.routing

import akka.actor.{ActorSystem, SupervisorStrategy}
import akka.dispatch.Dispatchers
import akka.routing._
import com.typesafe.config.Config

class IndexerPool(override val nrOfInstances: Int, override val resizer: Option[Resizer]) extends Pool {

  def this(config: Config) =
    this(
      nrOfInstances = config.getInt("nr-of-instances"),
      resizer = DefaultResizer.fromConfig(config))

  override def supervisorStrategy: SupervisorStrategy = Pool.defaultSupervisorStrategy

  override def createRouter(system: ActorSystem): Router = new Router(new IndexerRoutingLogic())

  override def routerDispatcher: String = Dispatchers.DefaultDispatcherId

}
