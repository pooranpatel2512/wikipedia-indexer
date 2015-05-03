package com.pooranpatel.settings

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}

/**
 * Settings provider for akka configuration
 */
object Settings extends ExtensionId[SettingsImpl] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): SettingsImpl = new SettingsImpl(system.settings.config)

  override def lookup(): ExtensionId[_ <: Extension] = Settings
}
