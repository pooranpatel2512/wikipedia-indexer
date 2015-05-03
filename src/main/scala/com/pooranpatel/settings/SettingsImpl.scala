package com.pooranpatel.settings

import akka.actor.Extension
import com.typesafe.config.Config

/**
 * Settings for akka configuration
 */
class SettingsImpl(config: Config) extends Extension {
  /**
   * number of wikipedia articles indexers
   */
  val nrIndexers = config.getInt("app.indexers")
}