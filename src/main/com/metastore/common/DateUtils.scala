package com.metastore.common

import org.joda.time.format.{ DateTimeFormat, DateTimeFormatter, ISODateTimeFormat }
import org.joda.time.{ DateTime, DateTimeZone }

import java.util.concurrent.TimeUnit

object DateUtils {

  val ISO8601Format: DateTimeFormatter = ISODateTimeFormat.dateTime()
  val simpleDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd")
  val simpleMonthFormat = DateTimeFormat.forPattern("yyyy-MM")
  val ISO8601JSTFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.forID("Asia/Tokyo"))
  val ISO8601UTCFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZoneUTC()
  val ISO8601UTCHourCutFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:00:00'Z'").withZoneUTC()
  val dateFormatInput = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH").withZoneUTC()
  def now(): DateTime = new DateTime(System.currentTimeMillis())

  def getCurrentDate(): String = simpleDateFormat.print(DateTime.now())
  def getCurrentMonth(): String = simpleMonthFormat.print(DateTime.now())

  def getRunDuration(startTime: Long, endTime: Long = System.currentTimeMillis()): Unit = {
    val startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(startTime)
    val endTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(endTime)
    val runDuration = Some(endTime - startTime)
    val executionDurationInSeconds = Some(endTimeInSeconds - startTimeInSeconds)

  }
}