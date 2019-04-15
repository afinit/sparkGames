package com.mrka

case class WineQuality(
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
) {
  def xVec: Vector[Double] =
    Vector(
      fixedAcidity,
      volatileAcidity,
      citricAcid,
      residualSugar,
      chlorides,
      freeSulfurDioxide,
      totalSulfurDioxide,
      density,
      pH,
      sulphates,
      alcohol,
    )

  def y: Double = quality
}

object WineQuality {
  def fromLines(lines: Iterator[String]): Iterator[WineQuality] = {
    lines.map { line =>
      val wqVals = line.split(";").map(_.trim)
      require(wqVals.length == 12, s"Line did not split into fields correctly: \n$line")

      try {
        WineQuality(
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
      catch {
        case e: Exception => 
          println(s"Unexpected character in line: $line")
          throw e
      }
    }
  }
}
