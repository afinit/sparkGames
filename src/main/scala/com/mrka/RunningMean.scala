package com.mrka


case class RunningMean(
  meanOpt: Option[Double] = None,
  length: Int = 0
) {
  def updateMean( nums: Vector[Double] ): RunningMean = {
    meanOpt match {
      case None if nums.length > 0 => RunningMean( Some(nums.reduce(_+_) / nums.length), nums.length )
      case None => RunningMean(None, 0)
      case Some(mean) => 
        val newLength = length + nums.length
        val newMean = length.toDouble / newLength * mean + nums.reduce(_+_) / newLength
        RunningMean(Some(newMean), newLength)
    }
  }
}
