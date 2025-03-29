package com.metastore.moksha.query

import com.metastore.adapter.logging.JobReport
import com.metastore.adapter.logging.JobReport._
import com.metastore.bookworm.MongoClient
import com.metastore.common.ElasticSearchUtils._
import com.metastore.common.FileUtils._
import com.metastore.common.{ AppConfig, DateUtils, FileUtils }
import com.metastore.moksha.RatServiceGroups
import org.apache.commons.lang.exception.ExceptionUtils
import scopt.OptionParser
import com.google.cloud.storage.StorageOptions
import org.slf4j.LoggerFactory

import scala.util.{ Failure, Success, Try }

object SGCTranscriber extends App {

  case class Options(executionDateHour: String = "", forceDeleteOutputPath: Boolean = false)

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def appArgs: OptionParser[Options] = new OptionParser[Options]("SGCTranscriber") {
    head("SGCTranscriber")
    opt[String]("executionDateHour")
      .required()
      .action((arg, option) => option.copy(executionDateHour = arg))
      .validate(x => {
        val ret = Try(DateUtils.dateFormatInput.parseDateTime(x))
        if (ret.isSuccess)
          success
        else
          failure("Invalid date format.Should be yyyy-MM-ddTHH")
      }).text("Date and Hour UTC is in yyyy-MM-ddTHH format")

    opt[Unit]("forceDeleteOutputPath")
      .action((arg, option) => option.copy(forceDeleteOutputPath = true))
      .text("Remove the existing path before (re)running.")
  }

  appArgs.parse(args, Options()) match {
    case None => throw new RuntimeException("fail to parse the params")
    case Some(opt) => run(opt)
  }

  def run(opt: Options): Unit = {
    val confKey = "app.conf"
    val envConfPath = System.getProperty(confKey)
    val conf = AppConfig.init(Option(envConfPath), confKey)
    val bookworm = new MokshaMongoClient(conf)
    val sgSuccessFile = "service_group.tsv"
    val scSuccessFile = "service_category.tsv"
    val failureFile = "failure.txt"
    val env = conf.getString("env")
    val dataCenter = conf.getString("data-center")
    // Env check
    val on_prem = conf.getString("data-center")

    if (on_prem == "false") {
      val gcProjectId = conf.getString("metastore.gcProjectId")
      val gcsBucketId = conf.getString("metastore.gcsBucketId")
      val sgOutputPath = conf.getString("metastore.sgcTranscriber.sgOutputPath") + "/" + "date_hour=" + opt.executionDateHour
      val scOutputPath = conf.getString("metastore.sgcTranscriber.scOutputPath") + "/" + "date_hour=" + opt.executionDateHour
      val storage = StorageOptions.newBuilder.setProjectId(gcProjectId).build.getService

      Try {
        val serviceGroups: Seq[RatServiceGroups] = bookworm.fetchServiceGroups()
        val serviceGroupMap: Map[Int, String] = serviceGroups
          .map(sg => {
            if (sg.acc.toInt == 7) (sg.acc.toInt, "rmsg")
            else (sg.acc.toInt, sg.serviceGroup)
          }).toMap

        val serviceCategoryMap: Map[Int, String] =
          serviceGroups
            .map { sg => (sg.acc.toInt, sg.serviceCategory) }
            .toMap
        (serviceGroupMap, serviceCategoryMap)
      } match {
        case Success((serviceGroupMap, serviceCategoryMap)) =>
          createObjectGs(gcsBucketId, sgOutputPath + "/" + sgSuccessFile, storage, toTsvLines(serviceGroupMap))
          createObjectGs(gcsBucketId, scOutputPath + "/" + scSuccessFile, storage, toTsvLines(serviceCategoryMap))
          println(s"Successfully processed. File created: $sgOutputPath/$sgSuccessFile")
          println(s"Successfully processed. File created: $scOutputPath/$scSuccessFile")
        case Failure(e) =>
          val errorMessage = ExceptionUtils.getStackTrace(e)
          createObjectGs(gcsBucketId, sgOutputPath + "/" + failureFile, storage, List(errorMessage))
          createObjectGs(gcsBucketId, scOutputPath + "/" + failureFile, storage, List(errorMessage))
          println(s"Failed processing. File created: $sgOutputPath/$failureFile")
          println(s"Failed processing. File created: $scOutputPath/$failureFile")
          println(s"Failure Reason: $errorMessage")
          sys.exit(1)
      }
    } else {
      val startTime = System.currentTimeMillis()
      val sgOutputPath = conf.getString("moksha.sgcTranscriber.sgOutputPath")
      val hadoopConfDir = conf.getString("moksha.hadoopConfDir")
      val hdfsPathPrefix = conf.getString("moksha.HDFSPathPrefix")
      val sgHdfsOutputPath = s"${hdfsPathPrefix}/${sgOutputPath}/date_hour=${opt.executionDateHour}"
      val scOutputPath = conf.getString("moksha.sgcTranscriber.scOutputPath")
      val scHdfsOutputPath = s"${hdfsPathPrefix}/${scOutputPath}/date_hour=${opt.executionDateHour}"

      // Hadoop configuration
      val hadoopConfiguration = FileUtils.initHadoopConf(hadoopConfDir, sgHdfsOutputPath)

      // For logging
      val taskName = this.getClass.getSimpleName.split("\\$").last
      val snapshot = Map("snapshot" -> "service_group and service_category")
      implicit val jobReport = JobReport(
        task = taskName,
        env = s"${env}-${dataCenter}",
        dataCenter = dataCenter,
        inputDateHourUTC = opt.executionDateHour,
        runStartTime = startTime,
        specific = snapshot,
        status = Start)

      Try {
        FileUtils.hdfsPreparePaths(hadoopConfiguration, s"$sgHdfsOutputPath/$sgSuccessFile", opt.forceDeleteOutputPath)
        FileUtils.hdfsPreparePaths(hadoopConfiguration, s"$sgHdfsOutputPath/$failureFile", opt.forceDeleteOutputPath)
        FileUtils.hdfsPreparePaths(hadoopConfiguration, s"$scHdfsOutputPath/$scSuccessFile", opt.forceDeleteOutputPath)
        FileUtils.hdfsPreparePaths(hadoopConfiguration, s"$scHdfsOutputPath/$failureFile", opt.forceDeleteOutputPath)

        val serviceGroups: Seq[RatServiceGroups] = bookworm.fetchServiceGroups()
        val serviceGroupMap: Map[Int, String] = serviceGroups
          .map(sg => {
            if (sg.acc.toInt == 7) (sg.acc.toInt, "rmsg")
            else (sg.acc.toInt, sg.serviceGroup)
          }).toMap

        val serviceCategoryMap: Map[Int, String] =
          serviceGroups
            .map { sg => (sg.acc.toInt, sg.serviceCategory) }
            .toMap

        (serviceGroupMap, serviceCategoryMap)
      } match {
        case Success((serviceGrouMap, serviceCategoryMap)) => {
          writeToHDFS(
            hadoopConfiguration = hadoopConfiguration,
            path = sgHdfsOutputPath,
            name = sgSuccessFile,
            lines = sgResult)
          writeToHDFS(
            hadoopConfiguration = hadoopConfiguration,
            path = scHdfsOutputPath,
            name = scSuccessFile,
            lines = scResult)

          LOG.info(s"Successfully processed. File created (HDFS) serviceGroupFile: ${sgHdfsOutputPath}/${sgSuccessFile}")
          LOG.info(s"Successfully processed. File created (HDFS) serviceCategoryFile: ${scHdfsOutputPath}/${scSuccessFile}")
          LOG.info(s"serviceCategoryMap: ${serviceCategoryMap}")
        }
        case Failure(e) => {
          val errorMessage = ExceptionUtils.getStackTrace(e)
          writeToHDFS(
            hadoopConfiguration = hadoopConfiguration,
            path = sgHdfsOutputPath,
            name = failureFile,
            lines = List(errorMessage))
          writeToHDFS(
            hadoopConfiguration = hadoopConfiguration,
            path = scHdfsOutputPath,
            name = failureFile,
            lines = List(errorMessage))

          LOG.info(s"Failed processing. File created (HDFS): ${sgHdfsOutputPath}/${failureFile}")
          LOG.info(s"Failed processing. File created (HDFS): ${scHdfsOutputPath}/${failureFile}")
          LOG.info(s"Failure Reason: ${errorMessage}")

          sys.exit(1)
        }
      }
    }
  }
}