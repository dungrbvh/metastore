package com.metastore.common
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import scala.collection.JavaConversions._

object AppConfig {
  private val LOG = Logger.getLogger(this.getClass.getName)

  /**
   *
   * @param conf
   * @param log
   */
  def showConfig(conf: Config, log: org.slf4j.Logger): Unit = {
    LOG.info("Configuration list:")

    conf.entrySet().toList
      .map(x => s"${x.getKey}=${x.getValue}")
      .sorted
      .foreach(c => LOG.info(s"\t${c}"))
  }

  /**
   *
   * @param envConfPath
   */
  def init(envConfPath: String): Config = {
    if (envConfPath == null) {
      LOG.info("-Dmoksha.conf is not given.")
      ConfigFactory.load()
    } else {
      val c = ConfigFactory.parseFileAnySyntax(new java.io.File(envConfPath))
      ConfigFactory.load(c).withFallback(ConfigFactory.load())
    }
  }

  /**
   * @param envConfPath Container for value at -D<key>=<value>
   * @param key         What to define on -D<key>
   */
  def init(envConfPath: Option[String], key: String): Config = {
    envConfPath
      .map(p => {
        LOG.info(s"Parsing ${p} as configuration")
        val c = ConfigFactory.parseFileAnySyntax(new java.io.File(p))
        LOG.debug(c)
        ConfigFactory.load(c).withFallback(ConfigFactory.load())
      })
      .getOrElse {
        LOG.info(s"-D${key} is not given. Loading default.")
        ConfigFactory.load()
      }
  }
  /** end of init(envConfPath: Option[String], key: String) */
}
/** end of AppConfig */
