package com.mrka


case class LinearModel(
  modelParams: Vector[Double],
  learnRate: Double = 0.001
) {
  def predict(xVec: Vector[Double]): Double =
    (1.0 +: xVec).zip(modelParams).map { case (x,w) => x*w }.reduce(_+_)
}

object LinearModel {
  def getGradient(
    model: LinearModel, 
    xBatch: Vector[Vector[Double]], 
    yBatch: Vector[Double]
  ): Vector[Double] = {
    // Using a simple least squares approach
    val y_estimate = xBatch.map(model.predict)
    val error: Vector[Double] = yBatch.zip(y_estimate).map { case (yB, yE) => yB - yE }
    val grads: Vector[Vector[Double]] = error.zip(xBatch).map { case (e, xVec) => (1.0 +: xVec).map(x => - x * e) }
    grads.reduce( (d1, d2) => d1.zip(d2).map { case (i1,i2) => i1+i2 } ).map( _ / xBatch.length )
  }

  def mse(yEstimate: Vector[Double], yActual: Vector[Double]): Double = 
    yEstimate.zip(yActual).map { case (est, act) => est - act }.map(x => x*x).reduce(_+_) / yEstimate.length

  def calcTestError(
    model: LinearModel,
    xBatch: Vector[Vector[Double]],
    yBatch: Vector[Double]
  ): Double = {
    val yEstimate = xBatch.map(model.predict)
    mse(yEstimate, yBatch)
  }

  def updateModel(
    model: LinearModel,
    xBatch: Vector[Vector[Double]], 
    yBatch: Vector[Double]
  ): LinearModel = {
    val gradient = getGradient(model, xBatch, yBatch)
    val newParams = model.modelParams.zip(gradient).map { case (m, g) => m - model.learnRate * g }
    model.copy(modelParams = newParams)
  }

}
