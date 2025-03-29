package com.metastore.common

import com.metastore.adapter.logging.JobReport
import com.metastore.adapter.logging.JobReport._

import java.util.concurrent.TimeUnit

object ElasticSearchUtils {
  def applicationsStartMessage(snapshot: Map[String, String])(implicit jobReport: JobReport): JobReport = {
    jobReport.copy(specific = snapshot + ("message" -> "Application started")
  }

  /**
   * Used when the application has ended successfully
   * @param outputPath
   * @param jobReport
   * @return
   */
    def applicationEndMessage(outputPath: String)(implicit jobReport: jobReport): jobReport = {
      applicationEndMessage(outputPath, Completed, "Application Completed")(jobReport)
    }

  def applicationEndMessage(
                             outputPath: String,
                             status: String,
                             message: String)(implicit jobReport: JobReport): JobReport = {

    val endTime = System.currentTimeMillis()
    val startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(jobReport.runStartTime)
    val endTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(endTime)
    val runDuration = Some(endTime - jobReport.runStartTime)
    val executionDurationInSeconds = Some(endTimeInSeconds - startTimeInSeconds)

    jobReport.copy(
      status = status,
      runEndTime = Some(endTime),
      runDuration = runDuration,
      executionDurationInSeconds = executionDurationInSeconds,
      specific = Map(
        "message" -> message,
        "outputPath" -> outputPath))
  }
}