package com.mrka


object SparkGames extends App {
  val groupSize = 25

  val redWineQualityLines = scala.io.Source.fromResource("winequality-red.csv").getLines()
  val redFirstLine = redWineQualityLines.next.split(";").toVector
  val redModelStart = LinearModel((0 to redFirstLine.length).map(_ => 0.0).toVector, 0.0001)
  val redWineQualityVec = WineQuality.fromLines(redWineQualityLines)


  val (redTrainedModel, redTestSet) = redWineQualityVec.grouped(groupSize).foldLeft((redModelStart, Vector.empty[WineQuality])) {
    case ((currModel, currTestSet), batch) if batch.length > 1 => 
      val trainSet = batch.tail
      val testSet = batch.head +: currTestSet
      val updatedModel = LinearModel.updateModel(currModel, trainSet.map(_.xVec).toVector, batch.map(_.y).toVector)
      val testError = LinearModel.calcTestError(updatedModel, testSet.map(_.xVec).toVector, testSet.map(_.y).toVector)
      println(testError)
      (updatedModel, testSet)

    case ((currModel, currTestSet), _) => (currModel, currTestSet)
  }
  println(redTrainedModel)

  val whiteWineQualityLines = scala.io.Source.fromResource("winequality-white.csv").getLines()
  val whiteFirstLine = whiteWineQualityLines.next.split(";").toVector
  val whiteModelStart = LinearModel((0 to whiteFirstLine.length).map(_ => 0.0).toVector, 0.00005)
  val whiteWineQualityVec = WineQuality.fromLines(whiteWineQualityLines)


  val (whiteTrainedModel, whiteTestSet) = whiteWineQualityVec.grouped(groupSize).foldLeft((whiteModelStart, Vector.empty[WineQuality])) {
    case ((currModel, currTestSet), batch) if batch.length > 1 => 
      val trainSet = batch.tail
      val testSet = batch.head +: currTestSet
      val updatedModel = LinearModel.updateModel(currModel, trainSet.map(_.xVec).toVector, batch.map(_.y).toVector)
      val testError = LinearModel.calcTestError(updatedModel, testSet.map(_.xVec).toVector, testSet.map(_.y).toVector)
      println(testError)
      (updatedModel, testSet)

    case ((currModel, currTestSet), _) => (currModel, currTestSet)
  }
  println(whiteTrainedModel)


  //val pHMean = wineQualityRedObjVec.grouped(groupSize).foldLeft(RunningMean()) {
    //case (acc, batch) => 
      //val accNew = acc.updateMean(batch.map(_.pH).toVector)
      //println(accNew)
      //accNew
  //}
}
