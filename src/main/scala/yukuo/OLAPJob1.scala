package yukuo

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

import com.github.tototoshi.csv.CSVWriter
import com.typesafe.scalalogging.slf4j.{LazyLogging, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Row, SparkSession, TiContext}
import yukuo.Config._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

class Job extends LazyLogging {
  private var resultFileName = ""
  private var dbName: String = ""
  private var resultDir: String = ""
  private var startDateTime: String = ""
  private var startOfInputDay: Long = _
  private var startOfInputNextDay: Long = _
  private var startOfInputNext3Days: Long = _
  private var writer: CSVWriter = _
  private val conditionChineseMap: mutable.Map[String, String] = mutable.Map()
  var conditions: List[String] = List()

  private val spark = SparkSession
    .builder()
    .appName(APP_NAME)
    .getOrCreate()

  private val conf = spark.sqlContext

  def initTiContext(): TiContext = {
    val ti = new TiContext(spark)
    ti.tidbMapDatabase(dbName)
    ti
  }

  def initConfig(): Unit = {
    startDateTime = conf.getConf(START_DATE, "")
    resultDir = conf.getConf(OUTPUT_FOLDER, "/")
    dbName = conf.getConf(DB_NAME, "chiji_db")
  }

  def setConfig(key: String, value: String): Unit = {
    conf.setConf(key, value)
  }

  def calculateTime(inputDate: Date): Unit = {
    val calendar = Calendar.getInstance()
    calendar.setTime(inputDate)
    startOfInputDay = calendar.getTimeInMillis
    logger.info("startOfInputDay:" + startOfInputDay)
    calendar.add(Calendar.DAY_OF_MONTH, 1)
    startOfInputNextDay = calendar.getTimeInMillis
    logger.info("startOfInputNextDay:" + startOfInputNextDay)
    calendar.add(Calendar.DAY_OF_MONTH, 2)
    startOfInputNext3Days = calendar.getTimeInMillis
    logger.info("startOfInputNext3Days:" + startOfInputNext3Days)
  }

  def prepareTime(): Unit = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")

    var inputDate: Date = null
    var inputDateStr = startDateTime
    do {
      try {
        inputDate = dateFormat.parse(inputDateStr)
      } catch {
        case _: ParseException => println("日期格式不合法,请重新输入")
      }
    } while (inputDate == null && {
        print("请输入日期(yyyyMMdd):")
        inputDateStr = StdIn.readLine()
        true
    })
    logger.info("Input date:" + inputDate.getTime)
    calculateTime(inputDate)
  }

  def run(): Unit = {
    time {
      initConfig()
      prepareTime()
      initTiContext()
      doJob()
    }(logger, "Total run time for app:" + APP_NAME)
  }

  def initWriter(): Unit = {
    try {
      writer = CSVWriter.open(resultAbsPath(), "GB2312")
    } catch {
      case e: Exception => logger.error("Opening file error:" + e.getMessage); throw e
    }
  }

  def prepareData(): Unit = {
    //取基础日志
    spark.sql(
      s"""select ceil((time-$startOfInputDay+1)/86400000) dayid , t1.*
         	| from CommonLog t1
         	| where time between $startOfInputDay and $startOfInputNext3Days order by time""".stripMargin)
      .createTempView("log3")

    //取新增用户
    spark.sql(
      s"""select first(playerUId) playerUId
         	| from log3
         	| where dayid = 1 and logtype=2010
         | group by playerUId""".stripMargin)
      .createTempView("new_user")

    logger.info("Temp view created successfully")
  }

  def time[R](block: => R)(logger: Logger, info: String): R = {
    logger.info(info)
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    logger.info(info + ", Elapsed time: " + (t1 - t0) / 1000.0 / 1000.0 / 1000.0 + "s")
    result
  }

  def fetchDayOne(): DataFrame = {
    // maybe we need to order by time because we may get strange results with select if we use index
    time {
      val df = spark.sql(
        s"""select *
           | from log3 where dayid = 1""".stripMargin)
      df.cache()
      logger.info("OLAP sql 1 get all data count:" + df.count())
      df
    }(logger, "OLAP sql 1")
  }

//  def groupByPlayerUId(): mutable.HashSet[Long] = {
  def groupByPlayerUId(): DataFrame = {
    time {
      val df = spark.sql(
        s"""select distinct(playerUId) playerUId
           | from log3""".stripMargin)
      df.cache()
      logger.info("OLAP sql 2 playerUId count:" + df.count())
      df.createOrReplaceTempView("players")
      df
//      var ret: mutable.HashSet[Long] = mutable.HashSet()
//      df.collect().foreach((f: Row) => ret += f.get(0).asInstanceOf[Long])
//      ret
    }(logger, "OLAP sql 2")
  }

//  implicit val sortIntegersByString = new Ordering[Int]{
//    override def compare(a: Int, b: Int) = a.toString.compare(b.toString)
//  }

  def runOLAP(): Unit = {

    // sql 1 create DataFrame
    var df: DataFrame = fetchDayOne()

    // sql 2
    val df2: DataFrame = groupByPlayerUId()

    // count number of playerUIds
    val countPlayerUId: Int = df2.count().asInstanceOf[Int]

    // Meaning of this order map is :
    // We are to calculate answer of number of users having events with the following in time order:
    // (1) -> (2, 3) -> (4, 5) -> (8) -> (9)
    // orderMap[3] = 1 means event(3) is in the 2nd order of event chain
    // note that event (6) is in cancelMap, it will cancel all events from cancelMap[6] -> orderMap[6]
    val orderMap: Map[Long, Int] = Map(
      1L -> 0,
      2L -> 1,
      3L -> 1,
      4L -> 2,
      5L -> 2,
      6L -> 4, // orderMap[6] = 4 with cancelMap[6] = 1 means event 6 will cancel all events from chain #1 to #4 [2,3,4,5,8,9]
      8L -> 3,
      9L -> 4
    )

    val cancelMap: Map[Long, Int] = Map(
      6L -> 1
    )

    // max event chain length
    val eventLength = 10
    // max game count
    val maxGameCount = 5

    // rdd reformat original data to (playerUId, (time, eventId))
    val rdd: RDD[(Long, (Long, Long))] = df.rdd.map((row: Row) => {
      for (i <- 0 until row.length)
        print(row.get(i) + " ")
      println()
      (row.getAs[Long](2), (row.getAs[Long](1), row.getAs[Long](4)))
    }).persist()

    // rdd2 group rdd by playerUId and sort by time
    // TODO: groupByKey is not safe if data is not balanced
    val rdd2: RDD[(Long, Iterable[(Long, Long)])] = rdd.repartition(countPlayerUId).groupByKey().sortBy(_._2).persist()

    // rdd3 do logic on each partition
    // result RDD will be (playerUId, Array(i ∈ N) which represents whether event chain #i is reached)
    val rdd3: RDD[(Long, Array[Long])] = rdd2.mapPartitions(
      (it: Iterator[(Long, Iterable[(Long, Long)])]) => {
        var ans = Array.empty[(Long, Array[Long])]
        it.foreach((f: (Long, Iterable[(Long, Long)])) => {
          // calculate for each playerUId
          val ret = Array.fill(eventLength * maxGameCount)(0L)
          val pid = f._1.asInstanceOf[Long]
          // game Id indicates the number of game of same user in this day
          var gameId = 0
          var tail = 0
          f._2.foreach((g: (Long, Long)) => {
            val logType = g._2.asInstanceOf[Long]
            val eventId = orderMap.get(logType)
            val jumpBack = cancelMap.get(logType)
            // lastGameIdx can be changed by gameId or anything else.
            var lastGameIdx = (gameId + 1) * (eventLength - 1)
            println("get data: " + g)
            // here is a simple logic calculates first game for each user
            // change by editing if-clause logic
            // here ret(lastGameIdx) == 0 indicates the first game is over.
            // you can add more games by adding offset for orderMap and cancelMap values
            // for example ret(0..5) is first game, ret(6..10) is second game, etc.
            // change lastGameIdx value to support more games in same day
            // we may simply update gameId when we reach `ret(lastGameIdx) > 0`
            // and don't forget to clear ret array for a new game.
            if (ret(lastGameIdx) == 0) eventId match {
              case Some(id) =>
                jumpBack match {
                  case Some(toPrev) =>
                    // Question: if event chain is 123456 ,
                    // event 12345 has finished and a cancel occurs for 3 to jump back to 2
                    // will it cancel events 4 and 5 ?
                    if (id >= tail && toPrev < tail) {
                      // clear events from toPrev to tail
                      for (i <- toPrev until tail) {
                        ret(i) = 0
                      }
                      // reset tail
                      tail = toPrev
                    }
                  case None =>
                    if (id == 0 || (id > 0 && ret(id) == 0  && ret(id - 1) > 0)) {
                      ret(id) = 1
                      tail = Math.max(tail, id + 1)
                    }
                }
              case None =>
              // invalid event
            }
          })
          val partialOutput = ret.mkString("[", ",", "]")
          println("get partial result for " + pid + ": " + partialOutput)
          ans = ans.:+(pid, ret)
        })
        ans.iterator
      }
    )

    // calculate answer for all users
    // result will be (playerUId, answer array)
    val rdd4 = rdd3.reduceByKey((a, b) => {
      val ret = Array.ofDim[Long](a.length)
      for (i <- a.indices) {
          ret(i) = a(i) + b(i)
      }
      ret
    }).persist()

    df = spark.createDataFrame(rdd3)

    writeToFile(df)

    logger.info("Finish all jobs, result saved to " + resultDir + resultFileName)
  }

  def writeToFile(df: DataFrame): Unit = {
    resultFileName = conf
      .getConf(RESULT_FILE_NAME, s"${new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date())}_job_result_from_$startOfInputDay.csv")
    initWriter()
    val result = ArrayBuffer.empty[ArrayBuffer[Any]]
    val title = ArrayBuffer.empty[Any]
    df.schema.fields.foreach((field: StructField) => {
      title += field.name
    })
    result += title

    df.collect().foreach((row: Row) => {
      val rowRes = ArrayBuffer.empty[Any]
      for (i <- 0 until row.length) {
        rowRes += row.get(i)
      }
      result += rowRes
    })

    logger.info(result.toString())

    // 写入转置的数据
    val length = result.head.size
    for (i <- 0 until length) {
      val rowRes = ArrayBuffer.empty[Any]
      for (j <- result.indices) {
        rowRes += result(j)(i)
      }
      writer.writeRow(rowRes)
    }
    // new line
    writer.writeRow(Seq())

    writer.flush()
    logger.info("Finish running condition:")

    writer.close()
  }

  def resultAbsPath(): String = {
    resultDir + resultFileName
  }

  def doJob(): Unit = {
    prepareData()
    runOLAP()
  }
}

object OLAPJobRunner {
  def main(args: Array[String]): Unit = {
    var check = !args.isEmpty
    val job = new Job()
    for (keyVal <- args) {
      val u = keyVal.split("=")
      if (u.length != 2) {
        check = false
      } else {
        val key = u(0)
        val value = u(1)
        job.setConfig(key, value)
      }
    }
    if (check) {
      job.run()
//      Thread.sleep(1000000000000L)
    } else {
      print("no time value found")
    }
  }
}