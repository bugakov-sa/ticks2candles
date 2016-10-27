import java.nio.file.Paths

import akka.actor._
import akka.event.Logging

import scala.collection.mutable

case class MainTask(inputDir: String, outputDir: String, converterPath: String, timeframes: List[Int])

case class ChildTask(inputFile: String, outputFile: String, converterFile: String, timeframes: List[Int])

case class ChildTaskReady(inputFile: String)

case class ChildTaskError(inputFile: String, e: Exception)

class ParentActor(val task: MainTask) extends Actor {
  val log = Logging(context.system, this)
  log.info("Started with {}", task)

  val files = mutable.Buffer[String]()
  try {
    val dirs = Paths.get(task.inputDir).toFile.listFiles()
    log.info("{} days", dirs.length)
    files ++= dirs.flatMap(f => f.listFiles).map(f => f.getAbsolutePath).toBuffer
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
    actor ! ChildTask(files(0), "", task.converterPath, task.timeframes)
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
    case msg: ChildTaskError =>
      proc_files -= msg.inputFile
      err_files += msg.inputFile
      log.error("Error at file " + msg.inputFile + " : " + msg.e.getMessage, msg.e)
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
    //convert to txt
    //calculate candles
  }
}

object Application extends App {

  val parentDir = Paths.get(System.getProperty("user.dir")).getParent.toString
  val workDir = "work"

  val task = MainTask(
    Paths.get(parentDir, workDir, "input").toString,
    Paths.get(parentDir, workDir, "output").toString,
    Paths.get(parentDir, workDir).toString,
    List(60, 5 * 60, 15 * 60, 60 * 60)
  )

  ActorSystem.create("ticks2candles").actorOf(Props(new ParentActor(task)), name = "Parent")
}