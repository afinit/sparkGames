package com.mrka

import org.apache.spark.sql.types.{StructType, StructField, DoubleType}
import org.apache.spark.rdd.RDD

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
  def fromLine(line: String): WineQuality = {
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

  def fromLines(lines: Iterator[String]): Iterator[WineQuality] = {
    lines.map(fromLine)
  }

  def fromRdd(rdd: RDD[String]): RDD[WineQuality] =
    rdd.mapPartitions( iter => iter.map(fromLine))

  val struct = StructType(
    StructField("fixedAcidity", DoubleType, false) ::
    StructField("volatileAcidity", DoubleType, false) ::
    StructField("citricAcid", DoubleType, false) ::
    StructField("residualSugar", DoubleType, false) ::
    StructField("chlorides", DoubleType, false) ::
    StructField("freeSulfurDioxide", DoubleType, false) ::
    StructField("totalSulfurDioxide", DoubleType, false) ::
    StructField("density", DoubleType, false) ::
    StructField("pH", DoubleType, false) ::
    StructField("sulphates", DoubleType, false) ::
    StructField("alcohol", DoubleType, false) ::
    StructField("quality", DoubleType, false) :: Nil )
}
