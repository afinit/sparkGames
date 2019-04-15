package com.mrka

import org.scalatest.{ FunSpec, Matchers }

class LinearModelSpec extends FunSpec with Matchers {

  describe("LinearModel predict") {
    it("Should compute the prediction correctly") {
      val model = LinearModel(Vector(1.0, 2.0, 3.0, 4.0))
      val xVec1 = Vector(1.0, 0.0, 0.0)
      val xVec2 = Vector(0.0, 2.0, 0.0)
      val xVec3 = Vector(0.0, 0.0, 1.0)

      model.predict(xVec1) shouldBe 3.0
      model.predict(xVec2) shouldBe 7.0
      model.predict(xVec3) shouldBe 5.0
    }
  }

  describe("LinearModel Object") {
    it("Should calculate gradient correctly") {
      val model3 = LinearModel(Vector(1.0, 2.0, 3.0, 4.0))
      val xBatch = Vector(
        Vector(2.0, 2.0, 2.0),
        Vector(2.0, 2.0, 2.0),
        Vector(2.0, 2.0, 2.0)
      )

      val yBatch = Vector(10.0,10.0,10.0)
      val actual = LinearModel.getGradient(model3, xBatch, yBatch)

      actual shouldBe Vector(9.0,18.0,18.0,18.0)
    }

    it("Should calculate mse correctly") {
      val yEstimate = Vector(1.0, 2.0, -3.0, 0.0)
      val yActual = Vector(0.0, 4.0, -1.0, 3.0)

      LinearModel.mse(yEstimate, yActual) shouldBe 4.5
    }

    it("Should calculate test error correctly") {
      val model = LinearModel(Vector(1.0, 2.0, 3.0, 4.0))
      val xBatch = Vector(
        Vector(1.0, 0.0, 0.0),
        Vector(0.0, 2.0, 0.0),
        Vector(0.0, 0.0, 1.0)
      )
      val yActual = Vector(2.0, 5.0, 6.0)

      LinearModel.calcTestError(model, xBatch, yActual) shouldBe 2.0
    }

    it("Should properly calculate a model update") {
      val model = LinearModel(Vector(1.0, 2.0, 3.0, 4.0), 0.001)
      val xBatch = Vector(
        Vector(1.0, 0.0, 0.0),
        Vector(0.0, 2.0, 0.0),
        Vector(0.0, 0.0, 1.0)
      )
      val yActual = Vector(0.0, 10.0, 8.0)

      LinearModel.updateModel(model, xBatch, yActual) shouldBe LinearModel(Vector(1.001, 1.999, 3.002, 4.001), 0.001)
    }
  }
}

