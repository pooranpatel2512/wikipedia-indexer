package com.pooranpatel.settings

import akka.actor.Extension
import com.typesafe.config.Config

class SettingsImpl(config: Config) extends Extension {
  val nrIndexers = config.getInt("app.indexers")
}