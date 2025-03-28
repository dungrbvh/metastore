package com.metastore.common
import com.twitter.finagle.{ Http, Service, SimpleFilter, http }
import com.twitter.util.{ Future, Await }
import org.json4s._
import org.json4s.native.JsonMethods._ // For parsing JSON (or use org.json4s.jackson.JsonMethods._ for Jackson)
import org.json4s.DefaultFormats
import org.slf4j.LoggerFactory
import java.lang.{ Integer => JInteger }
import java.util.{ Map => JMap }
import java.util.{ HashMap => JHashMap }
import scala.collection.mutable.{ Map }

// Vault result schema
case class body(data: data1, lease_duration: Int, auth: Any)
case class data1(data: data2, metadata: Any)
case class data2(user: String, password: String)
class Services(val vaultAddress: String, val vaultToken: String, val vaultPath: String) {

  protected val client = Http.client.newService(vaultAddress)

  implicit val formats: Formats = DefaultFormats

  private val logger = LoggerFactory.getLogger(this.getClass)

  def getVaultSecret: Future[Map[String, String]] = {
    val r = http.Request(http.Method.Get, vaultPath)
    r.headerMap.add("X-Vault-Token", vaultToken)
    r.headerMap.add("Content-Type", "application/json")
    r.host = s"${vaultAddress}"
    client(r).map { resp =>
      val map: Map[String, String] = Map.empty
      val res = parse(resp.contentString).extract[body]
      logger.info(res.toString)
      map += res.data.data.user -> res.data.data.password
      map
    }

  }

}