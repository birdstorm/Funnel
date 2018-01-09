package yukuo

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date, Properties}

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

  def initTiContext(): TiContext = {
    val ti = new TiContext(spark)
    ti.tidbMapDatabase(dbName)
    ti
  }

  def loadConf(conf: String): Properties = {
    val confStream = getClass.getClassLoader.getResourceAsStream(conf)
    val prop = new Properties()
    prop.load(confStream)
    prop
  }

  def initConfig(): Unit = {
    val prop: Properties = loadConf(CONFIG_NAME)
    resultDir = prop.getOrDefault(OUTPUT_FOLDER, "/").toString
    dbName = prop.getOrDefault(DB_NAME, "chiji_db").toString
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

  def prepareTime(t: String): Unit = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")

    var inputDate: Date = null
    var inputDateStr = t
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

  def run(t: String): Unit = {
    time {
      prepareTime(t)
      initConfig()
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
    // order by time because we may get strange results with select if we use index
    time {
      val df = spark.sql(
        s"""select *
           | from log3 where dayid = 1 order by time""".stripMargin)
      df.cache()
      logger.info("OLAP sql 1 get all data count:" + df.count())
      df
    }(logger, "OLAP sql 1")
  }

  // not used
  @deprecated
  def groupByPlayerUId(): mutable.HashSet[Long] = {
    time {
      val df = spark.sql(
        s"""select distinct(playerUId) playerUId
           | from log3""".stripMargin)
      df.cache()
      logger.info("OLAP sql 2 playerUId count:" + df.count())
      df.createOrReplaceTempView("players")
      var ret: mutable.HashSet[Long] = mutable.HashSet()
      df.collect().foreach((f: Row) => ret += f.get(0).asInstanceOf[Long])
      ret
    }(logger, "OLAP sql 2")
  }

//  implicit val sortIntegersByString = new Ordering[Int]{
//    override def compare(a: Int, b: Int) = a.toString.compare(b.toString)
//  }

  def runOLAP(): Unit = {

    //sql 1
    var df: DataFrame = fetchDayOne()

    // Meaning of this order map is :
    // We are to calculate answer of number of users having events with the following in time order:
    // (1) -> (2, 3) -> (4, 5) -> (8, 9)
    // orderMap[3] = 2 means event(3) is in the 2nd order of event chain
    // note that event (6) is in cancelMap, it will cancel all events from cancelMap[6] -> orderMap[6]
    val orderMap: Map[Long, Int] = Map(
      1L -> 0,
      2L -> 1,
      3L -> 1,
      4L -> 2,
      5L -> 2,
      6L -> 3,
      8L -> 3,
      9L -> 4
    )

    val cancelMap: Map[Long, Int] = Map(
      6L -> 1
    )

    val rdd: RDD[(Long, (Long, Long))] = df.rdd.map((row: Row) => {
      for (i <- 0 until row.length)
        print(row.get(i) + " ")
      println()
      (row.getAs[Long](2), (row.getAs[Long](1), row.getAs[Long](4)))
    }).persist()

    val rdd2: RDD[(Long, Iterable[(Long, Long)])] = rdd.groupByKey().sortBy(_._2).persist()

    val rdd3: RDD[(Long, Array[Long])] = rdd2.mapPartitions(
      (it: Iterator[(Long, Iterable[(Long, Long)])]) => {
        var ans = Array.empty[(Long, Array[Long])]
        it.foreach((f: (Long, Iterable[(Long, Long)])) => {
          val ret = Array.fill(10)(0L)
          val pid = f._1.asInstanceOf[Long]
          var tail = 0
          f._2.foreach((g: (Long, Long)) => {
            val logType = g._2.asInstanceOf[Long]
            val eventId = orderMap.get(logType)
            val jumpBack = cancelMap.get(logType)
            println("get data: " + g)
            eventId match {
              case Some(id) =>
                jumpBack match {
                  case Some(toPrev) =>
                    if (toPrev < tail) {
                      for (i <- toPrev until tail) {
                        ret(i) = 0
                      }
                      tail = toPrev
                    }
                  case None =>
                    if (id == 0 || (id > 0 /*&& ret(id) == 0 */ && ret(id - 1) > 0)) {
                      ret(id) = 1
                      tail = Math.max(tail, id)
                    }
                }
              case None =>
              // invalid event
            }
          })
          println("get partial result for " + pid + ": " + ret.map((x: Long) => x + " "))
          ans = ans.:+(pid, ret)
        })
        ans.iterator
      }
    )

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
    val prop: Properties = loadConf(CONFIG_NAME)
    resultFileName = prop
      .getOrDefault(RESULT_FILE_NAME, s"${new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date())}_job_result_from_$startOfInputDay.csv")
      .toString
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
    if (args.length >= 1) {
      new Job().run(args(0))
//      Thread.sleep(1000000000000L)
    } else {
      print("no time value found")
    }
  }
}