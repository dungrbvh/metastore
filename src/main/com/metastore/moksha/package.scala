package com.metastore
import com.metastore.common.DateUtils
import org.joda.time.DateTime
import org.json4s.JValue
import org.json4s.JsonDSL._

package object moksha {
  case class Dimension(
                      cases: Option[Seq[String]],
                      createdDate: Option[Long],
                      dataType: Option[String],
                      dimensionType: Option[String],
                      `else`: Option[String],
                      is_deleted: Option[Boolean],
                      is_activated: Option[Boolean],
                      key: String,
                      evarize_keys_variants: Option[String])
                      )
  case class RatServiceGroups(
                               acc: String,
                               accAid: String,
                               aid: String,
                               hiveSchema: String,
                               serviceCategory: String,
                               serviceGroup: String)
  type ReportStatusMessageType = (DateTime, ReportStatus, String, String)
  sealed class ReportStatus(val value: String)
}