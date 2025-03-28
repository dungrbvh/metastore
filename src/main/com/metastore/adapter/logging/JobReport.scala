package com.metastore.adapter.logging

import java.net.InetAddress

case class JobReport(
                      task: String,
                      status: String,
                      env: String,
                      dataCenter: String,
                      hostName: String = InetAddress.getLocalHost.getCanonicalHostName,
                      inputDateHourUTC: String,
                      runStartTime: Long,
                      runEndTime: Option[Long] = None,
                      runDuration: Option[Long] = None,
                      executionDurationInSeconds: Option[Long] = None,
                      statistics: Option[Map[String, Long]] = None,
                      specific: Map[String, String] = Map.empty)
                    )

object JobReport {
  val Completed = "completed"
  val Failed = "failed"
  val Running = "running"
  val Start = "start"
}