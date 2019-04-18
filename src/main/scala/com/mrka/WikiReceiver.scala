package com.mrka

import akka.NotUsed
import akka.actor.ActorSystem
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._
import scala.concurrent.ExecutionContext.Implicits.global

import akka.http.scaladsl.model.HttpMethods._

class WikiReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  private def receive() {
    implicit val actor = ActorSystem("wikiActor")
    implicit val mat = ActorMaterializer()

    val wikiUrl = "https://stream.wikimedia.org/v2/stream/recentchange"
    try {
      Http()
        .singleRequest( HttpRequest(GET, uri = Uri(wikiUrl)) )
        .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
        .foreach(_.runForeach(sse => store(sse.getData())))

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart(s"Error connecting to $wikiUrl", e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}
