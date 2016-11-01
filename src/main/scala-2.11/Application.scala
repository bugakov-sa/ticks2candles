/*
TODO:
1. Поддержать скачивание файлов по ftp
2. Логировать длительность работы
 */

import java.io.{File, PrintWriter}
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import akka.actor._
import com.typesafe.scalalogging.Logger

import scala.collection.mutable
import scala.concurrent.duration._
import scala.io.Source

case class Configuration(
                          inputFiles: List[String],
                          outputDir: String,
                          converterFile: String,
                          timeframes: List[Long],
                          threads: Int
                        )

object Configuration {

  private val log = Logger[Configuration]

  private val INPUT_DIR = "inputDir"
  private val OUTPUT_DIR = "outputDir"
  private val CONVERTER_PATH = "converterPath"
  private val TIMEFRAMES = "timeframes"
  private val THREADS = "threads"

  def read: Configuration = {

    val settings = readSettings

    checkSettings(settings)

    Configuration(
      listInputFiles(settings(INPUT_DIR)),
      settings(OUTPUT_DIR),
      settings(CONVERTER_PATH),
      parseTimeframes(settings(TIMEFRAMES)),
      parseThreads(settings(THREADS))
    )
  }

  private def findSettingsFile = {
    val files = Array("settings.txt", "conf\\settings.txt").
      map(path => (path, new File(path).exists)).
      filter(p => p._2).
      map(p => p._1)
    if (files.isEmpty) {
      throw new Exception("Not found settings.txt")
    }
    files(0)
  }

  private def readSettings = {
    val src = Source.fromFile(findSettingsFile)
    val settings = (for (kv <- src.getLines.map(ln => ln.split("=")) if kv.length == 2)
      yield (kv(0).trim, kv(1).trim)).toMap
    src.close

    log.info("Settings:")
    for ((k, v) <- settings) {
      log.info("{} = {}", k, v)
    }

    settings
  }

  private def checkSettings(settings: Map[String, String]) = {
    for (filePath <- List(INPUT_DIR, OUTPUT_DIR, CONVERTER_PATH)) {
      if (!settings.contains(filePath)) {
        throw new Exception("Not defined " + filePath)
      }
      if (!Paths.get(settings(filePath)).toFile.exists) {
        throw new Exception("Not found " + filePath + " " + settings(filePath))
      }
    }
    if (!settings.contains(TIMEFRAMES)) {
      throw new Exception("Not defined " + TIMEFRAMES)
    }
    try {
      parseTimeframes(settings(TIMEFRAMES))
    }
    catch {
      case e: Exception => {
        throw new Exception("Cannot parse " + TIMEFRAMES + " " + settings(TIMEFRAMES))
      }
    }
    if (!settings.contains(THREADS)) {
      throw new Exception("Not defined " + THREADS)
    }
    try {
      settings(THREADS).toInt
    }
    catch {
      case e: Exception => {
        throw new Exception("Cannot parse " + THREADS + " " + settings(THREADS))
      }
    }
  }

  private def listInputFiles(inputDir: String): List[String] = {
    val dirFiles = new File(inputDir).listFiles
    val res = dirFiles.
      filter(_.isFile).
      filter(_.getName.endsWith(".qsh")).
      map(_.getAbsolutePath).
      toList ::: dirFiles.
      filter(_.isDirectory).
      map(_.getAbsolutePath).
      flatMap(listInputFiles _).
      toList
    log.info("Found {} files", res.size)
    res
  }

  private def parseTimeframes(timeframes: String) = timeframes.
    split(" ").
    map(_.toLong).
    toList

  private def parseThreads(threads: String) = {
    if (threads.toInt <= 0) {
      Runtime.getRuntime.availableProcessors
    }
    else {
      threads.toInt
    }
  }
}

object Utils {
  val qshFileNamePattern = "OrdLog.(\\w+-\\d+.\\d+).(\\d{4}.\\d{1,2}.\\d{1,2}).qsh".r
}

class TimeFormatter {
  private val txtTimestampFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss.SSS")
  txtTimestampFormat.setTimeZone(TimeZone.getTimeZone("GMT"))

  def millisFromTxt(timeText: String) = txtTimestampFormat.parse(timeText).getTime

  private val csvTimestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  csvTimestampFormat.setTimeZone(TimeZone.getTimeZone("GMT"))

  def millis2Csv(millis: Long) = csvTimestampFormat.format(new Date(millis))
}

class Candle(val open: String) {
  var low = open
  var high = open
  var close = open

  def add(price: String) = {
    if (low.toFloat > price.toFloat) {
      low = price
    }
    if (high.toFloat < price.toFloat) {
      high = price
    }
    close = price
  }
}

class CandlesBatch(val timeframe: Long) {
  val candles = scala.collection.mutable.LinkedHashMap[Long, Candle]()

  def add(price: String, receiveMillis: Long, executeMillis: Long) = {
    val receiveInDayMillis = receiveMillis % (1 day).toMillis
    val startTradeInDayMillis = (10 hours).toMillis
    if (receiveInDayMillis >= startTradeInDayMillis) {
      val middleOfCandleMillis = executeMillis - executeMillis % timeframe + timeframe / 2
      if (candles.contains(middleOfCandleMillis)) {
        candles(middleOfCandleMillis).add(price)
      }
      else {
        candles(middleOfCandleMillis) = new Candle(price)
      }
    }
  }
}

object ParentActor {

  case class ChildTaskSuccess(inputFile: String)

  case class ChildTaskFailure(inputFile: String, e: Exception)

  def props(conf: Configuration) = Props(new ParentActor(conf))
}

object ChildActor {

  case class ChildTask(inputFile: String, outputFiles: Map[Long, String], converterFile: String)

}

class ParentActor(val conf: Configuration) extends Actor {

  private val log = Logger[ParentActor]

  import ChildActor._
  import ParentActor._

  val files = mutable.Buffer[String]()
  val procFiles = mutable.Buffer[String]()
  val successFiles = mutable.Buffer[String]()
  val failureFiles = mutable.Buffer[String]()

  files ++= conf.inputFiles

  private val childActorsCount: Int = math.min(conf.threads, files.size)
  log.info(" Starting {} child actors", childActorsCount)
  (1 to childActorsCount)
    .map(i => context.actorOf(Props[ChildActor].withDispatcher("my-pinned-dispatcher")))
    .foreach(sendChildTask _)

  def sendChildTask(actor: ActorRef) = {
    val Utils.qshFileNamePattern(code, date) = Paths.get(files(0)).getFileName.toString
    val outputFiles = conf.timeframes.map(timeframe => (
      timeframe,
      Paths.get(conf.outputDir, code, "$date%-$timeframe%.csv").toString)
    ).toMap
    Paths.get(conf.outputDir, code).toFile.mkdirs
    actor ! ChildTask(files(0), outputFiles, conf.converterFile)
    procFiles += files(0)
    files.remove(0)
  }

  override def receive: Receive = {
    case msg: ChildTaskSuccess =>
      procFiles -= msg.inputFile
      successFiles += msg.inputFile
      tryProcNextFile
    case msg: ChildTaskFailure =>
      procFiles -= msg.inputFile
      failureFiles += msg.inputFile
      log.error("Error at file " + msg.inputFile + " : " + msg.e.getMessage, msg.e)
      tryProcNextFile
  }

  def tryProcNextFile = {
    printReport
    if (files.isEmpty) {
      if (procFiles.isEmpty) {
        log.info("Failure files:")
        failureFiles.foreach(f => log.info(f))
        log.info("Terminating")
        context.system.terminate
      }
    }
    else {
      sendChildTask(sender)
    }
  }

  def printReport = {

    val percent = 100 * (successFiles.size + failureFiles.size).toFloat / conf.inputFiles.size

    log.info("Progress {} (%, success, failure, total)",
      Array(
        f"$percent%02.2f",
        successFiles.size,
        failureFiles.size,
        conf.inputFiles.size
      )
    )
  }
}

class ChildActor extends Actor {

  import ChildActor._
  import ParentActor._

  val timeFormatter = new TimeFormatter

  override def receive: Actor.Receive = {
    case task: ChildTask =>
      try {
        executeTask(task)
        sender ! ChildTaskSuccess(task.inputFile)
      }
      catch {
        case e: Exception =>
          sender ! ChildTaskFailure(task.inputFile, e)
      }
      finally {
        val txtFileOption = txtFile(task.inputFile)
        if (!txtFileOption.isEmpty) {
          new File(txtFileOption.get).delete
        }
      }
  }

  def executeTask(task: ChildTask) = {
    import sys.process._
    (task.converterFile + " " + task.inputFile) ! ProcessLogger(s => {})

    val txtFileOption = txtFile(task.inputFile)
    if (!txtFileOption.isEmpty) {
      val src = Source.fromFile(txtFileOption.get)
      val candlesBatches = task.outputFiles.map(p => (p._2, new CandlesBatch(p._1)))
      for (line <- src.getLines() if filterLine(line)) {
        val cells = line.split(";")
        val price: String = cells(7)
        val receiveMillis = timeFormatter.millisFromTxt(cells(0))
        val executeMillis = timeFormatter.millisFromTxt(cells(1))
        candlesBatches.values.foreach(_.add(price, receiveMillis, executeMillis))
      }
      src.close()
      candlesBatches.foreach(p => saveCandles2csv(p._2, p._1))
    }
  }

  def filterLine(line: String) = line.contains("Fill") && line.contains("Quote") && !line.contains("NonSystem")

  def txtFile(inputFile: String): Option[String] = {
    val inputFileName = Paths.get(inputFile).getFileName.toString
    val inputFilePath = Paths.get(inputFile).getParent
    val tempFileNames = inputFilePath.toFile.list.filter(name => {
      val Utils.qshFileNamePattern(code, date) = inputFileName
      name.contains(code) && name.contains(date) && name.endsWith(".txt")
    })
    if (tempFileNames.isEmpty) None
    else Some(Paths.get(inputFilePath.toString, tempFileNames(0)).toString)
  }

  def saveCandles2csv(candles: CandlesBatch, csvPath: String) = {
    val out = new PrintWriter(csvPath)
    out.write("t,open,high,low,close\n")
    for ((t, c) <- candles.candles) {
      out.write(timeFormatter.millis2Csv(t) + "," + c.open + "," + c.high + "," + c.low + "," + c.close + "\n")
    }
    out.close()
  }
}

object Application extends App {
  val conf = Configuration.read
  ActorSystem.create("ticks2candles").actorOf(ParentActor.props(conf))
}