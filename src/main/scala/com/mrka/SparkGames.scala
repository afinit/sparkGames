package com.mrka

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import scopt.OptionParser


case class SparkGamesArgs(
  runRedWine: Boolean = false,
  runWhiteWine: Boolean = false,
  runSparkRed: Boolean = false,
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
    val conf = new SparkConf().setMaster("local[1]").setAppName("sparkgames")
    val ssc = new StreamingContext(conf, Seconds(1))

    // This isn't actually streaming since we end up converting it to a Seq.. not the goal here. It turns out individual files aren't really
    // meant to be streamed in spark since they can't be chunked
    val redIterator = scala.io.Source.fromURL("http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv").getLines
    val redLines = redIterator.filterNot(_.contains("fix")).toSeq
    val rdd = ssc.sparkContext.parallelize(redLines, 1)
    processWineQuality(rdd)
    ssc.start()
    ssc.awaitTermination()
  }


  def processWineQuality(rdd: RDD[String]): Unit = {
    val modelStart = LinearModel((0 to 12).map(_ => 0.0).toVector, 0.0001)
    val qualities: RDD[WineQuality] = WineQuality.fromRdd(rdd)

    qualities.foreachPartition((iter: Iterator[WineQuality]) => trainWineQualityModel(iter, modelStart, groupSize) )
  }

  def printRunningMeanOfPh(lines: Iterator[WineQuality]): Unit = {
    val pHMean = lines.grouped(groupSize).foldLeft(RunningMean()) {
      case (acc, batch) => 
        val accNew = acc.updateMean(batch.map(_.pH).toVector)
        println(accNew)
        accNew
    }
  }
}
