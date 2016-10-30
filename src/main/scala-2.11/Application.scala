/*
TODO:
1. Писать логи в файл
2. Логировать длительность работы
3. Отвязаться от фиксированной структуры папок с входными данными
 */

import java.io.{File, PrintWriter}
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.{TimeZone, Date}

import akka.actor._
import akka.event.Logging

import scala.collection.mutable
import scala.io.Source

case class Configuration(inputFiles: List[String], outputDir: String, converterFile: String, timeframes: List[Long])

object Configuration {

  private val INPUT_DIR = "inputDir"
  private val OUTPUT_DIR = "outputDir"
  private val CONVERTER_PATH = "converterPath"
  private val TIMEFRAMES = "timeframes"

  def read: Configuration = {

    val settings = readSettings

    checkSettings(settings)

    Configuration(
      listInputFiles(settings(INPUT_DIR)),
      settings(OUTPUT_DIR),
      settings(CONVERTER_PATH),
      parseTimeframes(settings(TIMEFRAMES))
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

    println("Settings:")
    for ((k, v) <- settings) {
      println(k + "=" + v)
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
  }

  private def listInputFiles(inputDir: String) = {
    val res = Paths.get(inputDir).
      toFile.
      listFiles.
      flatMap(f => f.listFiles).
      map(f => f.getAbsolutePath).
      filter(n => n.endsWith(".qsh")).
      toList
    println("Found " + res.size + " files")
    res
  }

  private def parseTimeframes(timeframes: String) = timeframes.
    split(" ").
    map(s => s.toLong).
    toList
}

object Utils {
  val qshFileNamePattern = "OrdLog.(\\w+-\\d+.\\d+).(\\d{4}.\\d{1,2}.\\d{1,2}).qsh".r
}

case class ChildTask(inputFile: String, outputFiles: Map[Long, String], converterFile: String)

case class ChildTaskSuccess(inputFile: String)

case class ChildTaskFailure(inputFile: String, e: Exception)

class ParentActor(val conf: Configuration) extends Actor {
  val log = Logging(context.system, this)

  val files = mutable.Buffer[String]()
  val proc_files = mutable.Buffer[String]()
  val ok_files = mutable.Buffer[String]()
  val err_files = mutable.Buffer[String]()

  files ++= conf.inputFiles

  val processors = Runtime.getRuntime.availableProcessors
  log.info("Starting {} child actors", processors)

  (1 to math.min(processors, files.size))
    .map(i => context.actorOf(Props[ChildActor]))
    .foreach(a => sendChildTask(a))

  def sendChildTask(actor: ActorRef) = {
    val Utils.qshFileNamePattern(code, date) = Paths.get(files(0)).getFileName.toString
    val outputFiles = conf.timeframes.map(t => (t, Paths.get(conf.outputDir, code, date + "-" + t + ".csv").toString)).toMap
    Paths.get(conf.outputDir, code).toFile.mkdirs
    actor ! ChildTask(files(0), outputFiles, conf.converterFile)
    proc_files += files(0)
    files.remove(0)
  }

  override def receive: Receive = {
    case msg: ChildTaskSuccess =>
      proc_files -= msg.inputFile
      ok_files += msg.inputFile
      tryProcNextFile
    case msg: ChildTaskFailure =>
      proc_files -= msg.inputFile
      err_files += msg.inputFile
      log.error("Error at file " + msg.inputFile + " : " + msg.e.getMessage, msg.e)
      msg.e.printStackTrace()
      tryProcNextFile
  }

  def tryProcNextFile = {
    val processedCount = ok_files.size + err_files.size
    val processedFraction = processedCount.toFloat / conf.inputFiles.size
    val processedPercent = Math.round(processedFraction * 10000f) / 100f
    log.info("Report: processed {}/{} ~ {} %",
      processedCount, conf.inputFiles.size, processedPercent)
    if (files.isEmpty) {
      if (proc_files.isEmpty) {
        if (!err_files.isEmpty) {
          log.info("Report: failure files:")
          err_files.foreach(f => log.info(f))
        }
        log.info("Report: completed (total / success / failure) {} / {} / {} files",
          processedCount, ok_files.size, err_files.size)
        context.system.terminate
      }
    }
    else {
      sendChildTask(sender)
    }
  }
}

class ChildActor extends Actor {
  val log = Logging(context.system, this)
  log.debug("Started {}", context.self)

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
        candlesBatches.values.foreach(cb => cb.add(cells(0), cells(1), cells(7)))
      }
      src.close()
      candlesBatches.foreach(p => p._2.save2csv(p._1))
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
    if (tempFileNames.isEmpty) None else Some(Paths.get(inputFilePath.toString, tempFileNames(0)).toString)
  }

  val txtTimestampFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss.SSS")
  txtTimestampFormat.setTimeZone(TimeZone.getTimeZone("GMT"))

  def millisFromTxt(timeText: String) = txtTimestampFormat.parse(timeText).getTime

  val csvTimestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  csvTimestampFormat.setTimeZone(TimeZone.getTimeZone("GMT"))

  def millis2Csv(millis: Long) = csvTimestampFormat.format(new Date(millis))

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
    val candles = scala.collection.mutable.LinkedHashMap[String, ChildActor.this.Candle]()

    def add(receiveTime: String, exchangeTime: String, price: String) = {
      val receiveTimeHour = (millisFromTxt(receiveTime) % (24 * 60 * 60 * 1000)) / (60 * 60 * 1000)
      if (receiveTimeHour >= 10) {
        val candleTime = millis2Csv((millisFromTxt(exchangeTime) / timeframe) * timeframe + timeframe / 2)
        if (candles.contains(candleTime)) {
          candles(candleTime).add(price)
        }
        else {
          candles(candleTime) = new Candle(price)
        }
      }
    }

    def save2csv(csvPath: String) = {
      val out = new PrintWriter(csvPath)
      out.write("t,open,high,low,close\n")
      for ((t, c) <- candles) {
        out.write(t + "," + c.open + "," + c.high + "," + c.low + "," + c.close + "\n")
      }
      out.close()
    }
  }
}

object Application extends App {
  val conf = Configuration.read
  ActorSystem.create("ticks2candles").actorOf(Props(new ParentActor(conf)))
}