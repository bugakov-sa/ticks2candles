import java.io.{File, PrintWriter}
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.{TimeZone, Date}

import akka.actor._
import akka.event.Logging

import scala.collection.mutable
import scala.io.Source

object Utils {
  val qshFileNamePattern = "OrdLog.([a-zA-Z]+-\\d+.\\d+).(\\d{4}.\\d{1,2}.\\d{1,2}).qsh".r
  val txtTimestampFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss.SSS")
  txtTimestampFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
  val csvTimestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  csvTimestampFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
}

case class MainTask(inputDir: String, outputDir: String, converterPath: String, timeframes: List[Int])

case class ChildTask(inputFile: String, outputFiles: Map[Int, String], converterFile: String)

case class ChildTaskReady(inputFile: String)

case class ChildTaskError(inputFile: String, e: Exception)

class ParentActor(val task: MainTask) extends Actor {
  val log = Logging(context.system, this)
  log.info("Started with {}", task)

  val files = mutable.Buffer[String]()
  try {
    val dirs = Paths.get(task.inputDir).toFile.listFiles()
    log.info("{} days", dirs.length)
    files ++= dirs.flatMap(f => f.listFiles).map(f => f.getAbsolutePath).filter(n => n.endsWith(".qsh")).toBuffer
    log.info("{} files", files.size)
  }
  catch {
    case th: Throwable =>
      log.error("Error at creating index: " + th.getMessage, th)
      context.system.terminate
  }

  val processors: Int = Runtime.getRuntime.availableProcessors
  log.info("Starting {} child actors", processors)

  def sendChildTask(actor: ActorRef) = {
    val Utils.qshFileNamePattern(code, date) = Paths.get(files(0)).getFileName.toString
    val outputFiles = task.timeframes.map(t => (t, Paths.get(task.outputDir, code, date + "-" + t + ".csv").toString)).toMap
    Paths.get(task.outputDir, code).toFile.mkdirs
    actor ! ChildTask(files(0), outputFiles, task.converterPath)
    proc_files += files(0)
    files.remove(0)
  }

  val proc_files = mutable.Buffer[String]()

  (1 to math.min(processors, files.size))
    .map(i => context.actorOf(Props[ChildActor]))
    .foreach(a => sendChildTask(a))

  val ok_files = mutable.Buffer[String]()
  val err_files = mutable.Buffer[String]()

  override def receive: Receive = {
    case msg: ChildTaskReady =>
      proc_files -= msg.inputFile
      ok_files += msg.inputFile
      nextFile
    case msg: ChildTaskError =>
      proc_files -= msg.inputFile
      err_files += msg.inputFile
      log.error("Error at file " + msg.inputFile + " : " + msg.e.getMessage, msg.e)
      nextFile
  }

  def nextFile = {
    if (files.isEmpty) {
      if (proc_files.isEmpty) {
        log.info("Fail files {}", err_files)
        log.info("Report: total: {}, success: {}, fail: {} files", ok_files.size + err_files.size, ok_files.size, err_files.size)
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
  log.info("Started {}", context.self)

  override def receive: Actor.Receive = {
    case task: ChildTask =>
      log.debug("Received {}", task)
      try {
        executeTask(task)
        sender ! ChildTaskReady(task.inputFile)
      }
      catch {
        case th: Throwable =>
          sender ! ChildTaskError(task.inputFile, new Exception(th))
      }
  }

  def executeTask(task: ChildTask) = {
    import sys.process._
    (task.converterFile + " " + task.inputFile) !

    val _txtFile = txtFile(task.inputFile)
    try {
      task.outputFiles.foreach(p => calculateCandles(_txtFile, p._2, p._1))
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

  def calculateCandles(inputFile: String, outputFile: String, timeframe: Int) = {
    val candles = scala.collection.mutable.LinkedHashMap[String, ChildActor.this.Candle]()
    val src = Source.fromFile(inputFile)
    for (line <- src.getLines() if line.contains("Fill") && line.contains("Quote") && !line.contains("NonSystem")) {
      val cells = line.split(";")
      val priceText = cells(7)
      val exTime = (Utils.txtTimestampFormat.parse(cells(1)).getTime / timeframe) * timeframe + timeframe / 2
      val recTime = (Utils.txtTimestampFormat.parse(cells(0)).getTime / timeframe) * timeframe + timeframe / 2
      val h = (recTime % (24 * 60 * 60 * 1000)) / (60 * 60 * 1000)
      if (h >= 10) {
        val candleTime = Utils.csvTimestampFormat.format(new Date(exTime))
        if (candles.contains(candleTime)) {
          candles(candleTime).add(priceText)
        }
        else {
          candles(candleTime) = new Candle(priceText)
        }
      }
    }
    src.close()
    val out = new PrintWriter(outputFile)
    out.write("t,open,high,low,close\n")
    for ((t, c) <- candles) {
      out.write(t + "," + c.open + "," + c.high + "," + c.low + "," + c.close + "\n")
    }
    out.close()
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