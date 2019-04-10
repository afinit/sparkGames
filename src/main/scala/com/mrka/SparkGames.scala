package com.mrka

case class wineQuality(
  fixedAcidity: Double,
  volatileAcidity: Double,
  citricAcid: Double,
  residualSugar: Double,
  chlorides: Double,
  freeSulfurDioxide: Double,
  totalSulfurDioxide: Double,
  density: Double,
  pH: Double,
  sulphates: Double,
  alcohol: Double,
  quality: Double
)

object SparkGames extends App {
  val groupSize = 25

  val wineQualityRedLines = scala.io.Source.fromResource("winequality-red.csv").getLines()

  wineQualityRedLines.next
  val wineQualityRedObjVec = wineQualityRedLines.map {
    wq =>
      val wqVals = wq.split(";").map(_.trim)
      wineQuality(
        wqVals(0).toDouble, 
        wqVals(1).toDouble, 
        wqVals(2).toDouble, 
        wqVals(3).toDouble, 
        wqVals(4).toDouble, 
        wqVals(5).toDouble, 
        wqVals(6).toDouble, 
        wqVals(7).toDouble, 
        wqVals(8).toDouble, 
        wqVals(9).toDouble, 
        wqVals(10).toDouble, 
        wqVals(11).toDouble
      )
  }

  val pHMean = wineQualityRedObjVec.grouped(groupSize).foldLeft(RunningMean()) {
    case (acc, batch) => 
      val accNew = acc.updateMean(batch.map(_.pH).toVector)
      println(accNew)
      accNew
  }

  wineQualityRedObjVec.take(10).toVector.map(println)
  println(s"Mean pH: ${pHMean}")
}
