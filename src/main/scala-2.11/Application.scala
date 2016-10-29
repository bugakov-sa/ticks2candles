import java.io.{File, PrintWriter}
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.{TimeZone, Date}

import akka.actor._
import akka.event.Logging

import scala.collection.mutable
import scala.io.Source

object Utils {
  val qshFileNamePattern = "OrdLog.(\\w+-\\d+.\\d+).(\\d{4}.\\d{1,2}.\\d{1,2}).qsh".r
}

case class MainTask(inputDir: String, outputDir: String, converterPath: String, timeframes: List[Int])

case class ChildTask(inputFile: String, outputFiles: Map[Int, String], converterFile: String)

case class ChildTaskSuccess(inputFile: String)

case class ChildTaskFailure(inputFile: String, e: Exception)

class ParentActor(val task: MainTask) extends Actor {
  val log = Logging(context.system, this)
  log.info("Started with {}", task)

  val files = mutable.Buffer[String]()
  try {
    val dirs = Paths.get(task.inputDir).toFile.listFiles
    log.info("Found {} days", dirs.length)
    files ++= dirs.flatMap(f => f.listFiles).map(f => f.getAbsolutePath).filter(n => n.endsWith(".qsh"))
  }
  catch {
    case th: Throwable =>
      log.error("Error at creating index: " + th.getMessage, th)
      context.system.terminate
  }
  val totalFilesCount = files.size
  log.info("Found {} files", totalFilesCount)

  val proc_files = mutable.Buffer[String]()
  val ok_files = mutable.Buffer[String]()
  val err_files = mutable.Buffer[String]()

  def sendChildTask(actor: ActorRef) = {
    val Utils.qshFileNamePattern(code, date) = Paths.get(files(0)).getFileName.toString
    val outputFiles = task.timeframes.map(t => (t, Paths.get(task.outputDir, code, date + "-" + t + ".csv").toString)).toMap
    Paths.get(task.outputDir, code).toFile.mkdirs
    actor ! ChildTask(files(0), outputFiles, task.converterPath)
    proc_files += files(0)
    files.remove(0)
  }

  val processors = Runtime.getRuntime.availableProcessors
  log.info("Starting {} child actors", processors)
  (1 to math.min(processors, files.size))
    .map(i => context.actorOf(Props[ChildActor]))
    .foreach(a => sendChildTask(a))

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
    val processedFraction = processedCount.toFloat / totalFilesCount
    val processedPercent = Math.round(processedFraction * 10000f) / 100f
    log.info("Report: processed {}/{} ~ {} %", processedCount, totalFilesCount, processedPercent)
    if (files.isEmpty) {
      if (proc_files.isEmpty) {
        if (!err_files.isEmpty) {
          log.info("Report: failure files:")
          err_files.foreach(f => log.info(f))
        }
        log.info("Report: completed (total / success / failure) {} / {} / {} files"
          , processedCount, ok_files.size, err_files.size)
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
        case th: Throwable =>
          sender ! ChildTaskFailure(task.inputFile, new Exception(th))
      }
  }

  def executeTask(task: ChildTask) = {
    import sys.process._
    (task.converterFile + " " + task.inputFile) ! ProcessLogger(s => {})

    val _txtFile = txtFile(task.inputFile)
    try {
      val src = Source.fromFile(_txtFile)
      val candlesBatches = task.outputFiles.map(p => (p._2, new CandlesBatch(p._1)))
      for (line <- src.getLines() if line.contains("Fill") && line.contains("Quote") && !line.contains("NonSystem")) {
        val cells = line.split(";")
        candlesBatches.values.foreach(cb => cb.add(cells(0), cells(1), cells(7)))
      }
      src.close()
      candlesBatches.foreach(p => p._2.save2csv(p._1))
    }
    finally {
      new File((_txtFile)).delete()
    }
  }

  def txtFile(inputFile: String): String = {
    val inputFileName = Paths.get(inputFile).getFileName.toString
    val inputFilePath = Paths.get(inputFile).getParent
    val tempFileName = inputFilePath.toFile.list.filter(name => {
      val Utils.qshFileNamePattern(code, date) = inputFileName
      name.contains(code) && name.contains(date) && name.endsWith(".txt")
    })(0)
    Paths.get(inputFilePath.toString, tempFileName).toString
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

  class CandlesBatch(val timeframe: Int) {
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

  val parentDir = Paths.get(System.getProperty("user.dir")).getParent.toString
  val workDir = "work"

  val task = MainTask(
    Paths.get(parentDir, workDir, "input").toString,
    Paths.get(parentDir, workDir, "output").toString,
    Paths.get(parentDir, workDir, "qsh2txt.exe").toString,
    List(60 * 60 * 1000, 15 * 60 * 1000, 5 * 60 * 1000, 60 * 1000)
  )

  ActorSystem.create("ticks2candles").actorOf(Props(new ParentActor(task)), name = "Parent")
}