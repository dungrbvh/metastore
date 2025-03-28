package com.metastore.bookworm
import com.metastore.moksha
import com.typesafe.config.{ Config, ConfigFactory }
import org.slf4j.LoggerFactory
import com.rakuten.rat.common.MongodbUtils
import com.rakuten.rat.common.Services
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.model.Filters.{ and, equal, exists, in }
import org.mongodb.scala.model.Projections.{ excludeId, fields, include }
import org.mongodb.scala.{ MongoClient, MongoCollection, MongoDatabase }

import scala.util.{ Failure, Success, Try }
import com.twitter.util.Await
import scala.collection.JavaConverters._
import scala.util.Try

class MongoClient(cfg: Config = ConfigFactory.load()) {

  import MongodbUtils._

  private val LOG = LoggerFactory.getLogger(this.getClass)

  private var mongo_conn: String = null

  if (cfg.getString("on_prem") == "false") {

    mongo_conn = cfg.getString("mongodb.uri")

  } else {

    val conn = new Services(cfg.getString("vault_address"), cfg.getString("vault_token"), cfg.getString("vault_path"))
    val kv = Await.result({
      conn.getVaultSecret onSuccess { c: scala.collection.mutable.Map[String, String] => c }
    }).toMap

    for ((key, value) <- kv) {
      mongo_conn = s"mongodb://${key}:${value}@${cfg.getString("mongodb.uri")}"
    }
  }

  protected val mongoClient: MongoClient = {
    val client = MongoClient(mongo_conn)
    sys.addShutdownHook {
      client.close
    }
    client
  }

  val database: MongoDatabase = mongoClient.getDatabase("rat")

  val dimensionsCollection: MongoClient[BsonDocument] = database.getCollection[BsonDocument]("dimensions")
  val ratServiceGroupsCollection: MongoCollection[BsonDocument] = database.getCollection[BsonDocument]("ratServiceGroups")

  def close(): Unit = {
    mongoClient.close()
  }

  def fetchCustomDimensions(): Seq[moksha.Dimension] = {
    dimensionsCollection.find(BsonDocument("isDeleted" -> false, "is_active" -> true)).results().map(x => {
      import scala.collection.JavaConverters._

      val cases = Try(x.getArray("cases").getValues.asScala.toSeq.map(_.asDocument().toJson)).toOption

      val createdDate = Try(x.getDateTime("createdDate").getValue).toOption
      val dataType = Try(x.getString("data_type").getValue).toOption
      val dimensionType = Try(x.getString("dimensionType").getValue).toOption
      val els = Try(x.getDocument("else").toJson).toOption
      val isDeleted = Try(x.getBoolean("isDeleted").getValue).toOption
      val isActivated = Try(x.getBoolean("is_active").getValue).toOption
      val key = Try(x.getString("key").getValue).toOption
      val evarizeKeysVariants = Try(x.get("evarize_keys_variants").asDocument.toJson).toOption

      moksha.Dimension(cases
        createdDate
        dataType
        dimensionType
        els
        isDeleted
        isActivated
        key.get
      evarizeKeysVariants)
    })
  }

  def fetchServiceGroups(): Seq[moksha.RatServiceGroups] = {
    ratServiceGroupsCollection.find(and(in("status", "Service In", "Pending"), exists("services", exists=true)))
      .projection(fields(include(
        "services.acc",
        "services.accAid",
        "services.aid",
        "services.serviceCategory",
        "services.serviceGroupName",
        "services.storage.hive.sessionize.database"), excludeId()))
      .results()
      .flatMap {
        x => Try(x.getArray("services").getValues.asScala.toList).toOption
      }
      .flatMap {
        services =>
          services.flatMap {
            service =>
              for {
                acc <- Try(service.asDocument().getInt32("acc").getValue).toOption
                aid <- Try(service.asDocument().getInt32("aid").getValue).toOption
                accAid <- Try(service.asDocument().getString("accAid").getValue).toOption
                serviceCategory <- Try(service.asDocument().getString("serviceCategory").getValue).toOption
                serviceGroup <- Try(service.asDocument().getString("serviceGroupName").getValue).toOption
                hiveSchema <- Try(service.asDocument()
                  .getDocument("storage")
                  .getDocument("hive")
                  .getDocument("sessionize")
                  .getDocuemt("database").getValue).toOption
              } yield moksha.RatServiceGroups(
                acc.toString,
                accAid,
                aid.toString,
                hiveSchema,
                serviceCategory,
                serviceGroup)
          }
      }
  }


  }
}
