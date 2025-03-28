package com.metastore.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.security.UserGroupInformation

import java.io.{ BufferedWriter, File, FileWriter, PrintWriter }
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import org.apache.log4j.Logger

object FileUtils {

  private val LOG = Logger.getLogger(this.getClass.getName)

  def initHadoopConf(hadoopConfDir: String, hdfsPathPrefix: String): Configuration = {
    val conf = new Configuration()
    val coreSiteXml = hadoopConfDir + "/core-site.xml"
    val hdfsSiteXml = hadoopConfDir + "/hdfs-site.xml"

    conf.addResource(new Path(coreSiteXml))
    conf.addResource(new Path(hdfsSiteXml))

    conf.set("fs.defaultFS", hdfsPathPrefix)
    conf.set("fs.default.name", hdfsPathPrefix)

    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromSubject(null)
    conf
  }

  def toTsvLines(map: Map[_, _]): Iterable[String] =
    map.map { case (key, value) => s"$key\t$value" }

  /**
   * Writes to HDFS
   *
   * @param hadoopConfiguration
   * @param path
   * @param name
   * @param lines
   */
  def writeToHDFS(hadoopConfiguration: Configuration, path: String, name: String, lines: Iterable[String]): Unit = {
    val fs = FileSystem.get(hadoopConfiguration)
    val fos = fs.create(new Path(s"${path}/${name}"))
    val writer = new PrintWriter(fos)
    writer.write(lines.mkString("\n"))
    writer.close()
    fos.close()
  }

  /**
   * Will delete paths in HDFS if exists
   *
   * @param path target path to check
   * @param isDeletePath this path will be deleted if set to `true`
   */
  def hdfsPreparePaths(hadoopConfiguration: Configuration, path: String, isDeletePath: Boolean): Unit = {

  }
}