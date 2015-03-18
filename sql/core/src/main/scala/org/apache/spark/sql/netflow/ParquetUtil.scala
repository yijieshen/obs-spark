package org.apache.spark.sql.netflow

import org.apache.hadoop.fs.{Path, FileStatus, FileSystem}
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.ArrayBuffer

object ParquetUtil {


  val minuteInterval = 10 //These should be read from configuration
  val netflowBaseDir = "hdfs://localhost:9000/netflow"
  val netflowBasePath = new Path(netflowBaseDir)

  val fs = netflowBasePath.getFileSystem(sc.hadoopConfiguration)

  val DHM_TIME = "yyyy-MM-dd|HH|mm"
  val DH_TIME = "yyyy-MM-dd|HH"
  val D_TIME = "yyyy-MM-dd"
  val FEILD_DELIMITER = '|'

  val dhmFormat = DateTimeFormat.forPattern(DHM_TIME)
  val dhFormat = DateTimeFormat.forPattern(DH_TIME)
  val dFormat = DateTimeFormat.forPattern(D_TIME)

  val sc: SparkContext = ???

  def getPath(parentPath: Path, curStr: String) = new Path(parentPath, new Path(curStr))

  def getPath(parentDir: String, curStr: String) = getPath(new Path(parentDir), curStr)

  /**
   * netflowBaseDir/2015-01-02/12/20/1.parquet
   *                                /2.parquet
   *                             /40
   *                             /60
   *                          /13/20
   *                             /40
   *               .................
   *               /2015-01-03/00/20
   *
   *
   * @param startTime
   * @param endTime
   * @return
   */
  def filePruning(startTime: String, endTime: String): Array[String] = {

    val startSplits = startTime.split(FEILD_DELIMITER)
    val (startDate, startHour, startMinute) = (startSplits(0), startSplits(1), startSplits(2))
    val endSplits = endTime.split(FEILD_DELIMITER)
    val (endDate, endHour, endMinute) = (endSplits(0), endSplits(1), endSplits(2))


    val dayStart = DateTime.parse(startDate, dFormat)
    val dayEnd = DateTime.parse(endDate, dFormat)

    val wholeDayDirs: Array[Path] = getTimeRange(dayStart.plusDays(1), dayEnd.minusDays(1))
      .map(dt => getPath(netflowBasePath, dFormat.print(dt)))
    val wholeDayPaths = wholeDayDirs.flatMap(path => fs.listStatus(path))
      .flatMap(fst => fs.listStatus(fst.getPath))
      .flatMap(fst => fs.listStatus(fst.getPath))

    val (sh, sm, eh, em) = (startHour.toInt, startMinute.toInt, endHour.toInt, endMinute.toInt)
    val startDayWholeHours = if (sm == 0) (sh to 24).toArray else (sh + 1 to 24).toArray
    val endDayWholeHours = (0 to eh - 1).toArray
    val wholehourDirs: Array[Path] =
      startDayWholeHours.map(i => new Path(getPath(netflowBasePath, startDate), i.toString)) ++
      endDayWholeHours.map(i => new Path(getPath(netflowBasePath, endDate), i.toString))

    val wholehourPaths = wholehourDirs.flatMap(path => fs.listStatus(path))
      .flatMap(fst => fs.listStatus(fst.getPath))

    val startMinutes = getStartMinutes(sm)
    val endMinutes = getEndMinutes(em)

    val partDirs =
      startMinutes.map(i => getPath(getPath(getPath(netflowBasePath, startDate), startHour), i.toString)) ++
      endMinutes.map(i => getPath(getPath(getPath(netflowBasePath, endDate), endHour), i.toString))

    val partPaths = partDirs.flatMap(path => fs.listStatus(path))

    (wholeDayPaths ++ wholehourPaths ++ partPaths).map(_.getPath.toString)

  }

  def getStartMinutes(start: Int): Array[Int] = {
    val ab = new ArrayBuffer[Int]
    var tmp = 60
    while (tmp > start) {
      ab += tmp
      tmp = tmp - minuteInterval
    }
    ab.toArray
  }

  def getEndMinutes(end: Int): Array[Int] = {
    val ab = new ArrayBuffer[Int]
    var tmp = 0
    while (tmp <= end) {
      tmp = tmp + minuteInterval
      ab += tmp
    }
    ab.toArray
  }

  /**
   *
   * @param dStart
   * @param dEnd
   * @return
   */
  def getTimeRange(dStart: DateTime, dEnd: DateTime): Array[DateTime] = {
    val dr = new ArrayBuffer[DateTime]
    var tmp = dStart
    while (tmp.isBefore(dEnd) || tmp.equals(dEnd)) {
      dr += tmp
      tmp = tmp.plusDays(1)
    }
    dr.toArray
  }

}
