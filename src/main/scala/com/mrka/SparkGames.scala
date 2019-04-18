package com.mrka

import io.circe.parser.decode

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.http.scaladsl.unmarshalling.Unmarshal
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.rdd.RDD
import scopt.OptionParser
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source

import scala.concurrent.ExecutionContext.Implicits.global

import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._

case class SparkGamesArgs(
  runRedWine: Boolean = false,
  runWhiteWine: Boolean = false,
  runSparkRed: Boolean = false,
  runPhRunningMean: Boolean = false,
  runSparkWikiEdits: Boolean = false,
  runWikiFirehose: Boolean = false,
  groupSize: Int = 25
)

object SparkGames extends App {
  private val parser = new OptionParser[SparkGamesArgs]("SparkGames"){
    opt[Unit]("runRedWine")
      .text("Run Red Wine Linear Model (from file iterator)")
      .action((_, args) => args.copy(runRedWine = true))

    opt[Unit]("runWhiteWine")
      .text("Run White Wine Linear Model (from file iterator)")
      .action((_, args) => args.copy(runWhiteWine = true))

    opt[Unit]("runSparkRed")
      .text("Run Red Wine Linear Model with Spark")
      .action((_, args) => args.copy(runSparkRed = true))

    opt[Unit]("runPhRunningMean")
      .text("Run Ph Running Mean (from file iterator)")
      .action((_, args) => args.copy(runPhRunningMean = true))

    opt[Unit]("runSparkWikiEdits")
      .text("Run Spark Wiki Edits Reader")
      .action((_, args) => args.copy(runSparkWikiEdits = true))

    opt[Unit]("runWikiFirehose")
      .text("Run Wiki Firehose to watch edits.. only uses akka http")
      .action((_, args) => args.copy(runWikiFirehose = true))

    opt[Int]("groupSize")
      .text("Batch size of runs")
      .action((grpSize, args) => args.copy(groupSize = grpSize))

    help("help").text("prints this usage text")
  }

  def parseArgs(args: Array[String]): SparkGamesArgs =
    parser.parse(args, SparkGamesArgs()) match {
      case Some(argsInstance) => argsInstance
      case None => throw new Exception("Please try again with different arguments")
    }

  val parsedArgs = parseArgs(args)
  val groupSize = parsedArgs.groupSize
  
  if(parsedArgs.runRedWine) runRedWine
  if(parsedArgs.runWhiteWine) runWhiteWine
  if(parsedArgs.runSparkRed) runSparkRed
  if(parsedArgs.runPhRunningMean) runPhRunningMean
  if(parsedArgs.runSparkWikiEdits) runSparkWikiEdits
  if(parsedArgs.runWikiFirehose) runWikiFirehose

  def trainWineQualityModel(iter: Iterator[WineQuality], modelStart: LinearModel, groupSize: Int): (LinearModel, Vector[WineQuality]) = {
    iter.grouped(groupSize).foldLeft((modelStart, Vector.empty[WineQuality])) {
      case ((currModel, currTestSet), batch) if batch.length > 1 => 
        val trainSet = batch.tail
        val testSet = batch.head +: currTestSet
        val updatedModel = LinearModel.updateModel(currModel, trainSet.map(_.xVec).toVector, batch.map(_.y).toVector)
        val testError = LinearModel.calcTestError(updatedModel, testSet.map(_.xVec).toVector, testSet.map(_.y).toVector)
        println(testError)
        (updatedModel, testSet)

      case ((currModel, currTestSet), _) => (currModel, currTestSet)
    }
  }

  def runRedWine: Unit = {
    val redWineQualityLines = scala.io.Source.fromResource("winequality-red.csv").getLines()
    val redFirstLine = redWineQualityLines.next.split(";").toVector
    val redModelStart = LinearModel((0 to redFirstLine.length).map(_ => 0.0).toVector, 0.0001)
    val redWineQualityIter = WineQuality.fromLines(redWineQualityLines)

    val (redTrainedModel, redTestSet) = trainWineQualityModel(redWineQualityIter, redModelStart, groupSize)
    println(redTrainedModel)
  }

  def runWhiteWine: Unit = {
    val whiteWineQualityLines = scala.io.Source.fromResource("winequality-white.csv").getLines()
    val whiteFirstLine = whiteWineQualityLines.next.split(";").toVector
    val whiteModelStart = LinearModel((0 to whiteFirstLine.length).map(_ => 0.0).toVector, 0.00005)
    val whiteWineQualityIter = WineQuality.fromLines(whiteWineQualityLines)

    val (whiteTrainedModel, whiteTestSet) = trainWineQualityModel(whiteWineQualityIter, whiteModelStart, groupSize)
    println(whiteTrainedModel)
  }

  def runSparkRed: Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("sparkgames")
    val ssc = new StreamingContext(conf, Seconds(1))

    // This isn't actually streaming since we end up converting it to a Seq.. not the goal here. It turns out individual files aren't really
    // meant to be streamed in spark since they can't be chunked
    val redIterator = scala.io.Source.fromURL("http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv").getLines
    val redLines = redIterator.filterNot(_.contains("fix")).toSeq
    val rdd = ssc.sparkContext.parallelize(redLines, 1)
    processWineQuality(rdd)
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }


  def processWineQuality(rdd: RDD[String]): Unit = {
    val modelStart = LinearModel((0 to 12).map(_ => 0.0).toVector, 0.0001)
    val qualities: RDD[WineQuality] = WineQuality.fromRdd(rdd)

    qualities.foreachPartition((iter: Iterator[WineQuality]) => trainWineQualityModel(iter, modelStart, groupSize) )
  }

  def runPhRunningMean: Unit = {
    val rawLines = scala.io.Source.fromResource("winequality-red.csv").getLines()
    val firstLine = rawLines.next.split(";").toVector
    val wqIter = WineQuality.fromLines(rawLines)

    val pHMean = wqIter.grouped(groupSize).foldLeft(RunningMean()) {
      case (acc, batch) => 
        val accNew = acc.updateMean(batch.map(_.pH).toVector)
        println(accNew)
        accNew
    }
    println(pHMean)
  }

  def runSparkWikiEdits: Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("sparkgames")
    val ssc = new StreamingContext(conf, Seconds(1))

    val wikiRec = new WikiReceiver
    val stream = ssc.receiverStream(wikiRec)
    stream.foreachRDD { rdd => 
      val eventRdd = parseJson(rdd)
      handleEvents(eventRdd)
    }
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  def runWikiFirehose: Unit = {
    implicit val actor = ActorSystem("wikiActor")
    implicit val mat = ActorMaterializer()

    val wikiUrl = "https://stream.wikimedia.org/v2/stream/recentchange"

    Http()
      .singleRequest( HttpRequest(GET, uri = Uri(wikiUrl)) )
      .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
      .foreach(_.runForeach(sse => println(sse.getData())))
  }


  def parseJson(rdd: RDD[String]): RDD[WikiEditEvent] = {
    rdd.mapPartitions{ iter =>
      iter.map{ eventString =>
        decode[WikiEditEvent](eventString) match {
          case Right(e) => Some(e)
          case Left(_) =>
            println(s"Could not parse, may be log event: $eventString")
            None
        }
      }.collect {
        case Some(e) => e
      }
    }
  }

  def handleEvents(eventRdd: RDD[WikiEditEvent]): Unit = {
    eventRdd.foreachPartition { iter => 
      iter.foreach { event => 
        println(s"SUCCESSFUL EVENT: $event") 
      }
    }
  }
}
