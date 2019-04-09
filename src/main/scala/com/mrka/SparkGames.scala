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
  val wineQualityRedLines = scala.io.Source.fromResource("winequality-red.csv").getLines()

  val wineQualityRedObjVec = wineQualityRedLines.toVector.tail.map {
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

  wineQualityRedObjVec.take(10).map(println)
}
