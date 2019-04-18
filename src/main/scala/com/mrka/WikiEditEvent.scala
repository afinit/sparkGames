package com.mrka

import io.circe.{ Encoder, Decoder, HCursor, Json }
import io.circe.syntax._


case class WikiEventMeta(
  topic: String,
  domain: String
)

object WikiEventMeta {
  implicit val encodeWikiEventMeta: Encoder[WikiEventMeta] = new Encoder[WikiEventMeta] {
    final def apply(s: WikiEventMeta): Json = Json.obj(
      ("topic", Json.fromString(s.topic)),
      ("domain", Json.fromString(s.domain))
    )
  }

  implicit val decodeWikiEventMeta: Decoder[WikiEventMeta] = new Decoder[WikiEventMeta] {
    final def apply(c: HCursor): Decoder.Result[WikiEventMeta] = {
      for {
        topic <- c.downField("topic").as[String]
        domain <- c.downField("domain").as[String]
      } yield WikiEventMeta(topic, domain)
    }
  }
}

case class WikiEditEventLength(
  oldLen: Option[Int],
  newLen: Option[Int]
)

object WikiEditEventLength {
  implicit val encodeWikiEditEventLength: Encoder[WikiEditEventLength] = new Encoder[WikiEditEventLength] {
    final def apply(s: WikiEditEventLength): Json = Json.obj(
      ("old", s.oldLen match {
        case Some(v) => Json.fromInt(v)
        case None => Json.Null
      }),
      ("new", s.newLen match {
        case Some(v) => Json.fromInt(v)
        case None => Json.Null
      })
    )
  }

  implicit val decodeWikiEditEventLength: Decoder[WikiEditEventLength] = new Decoder[WikiEditEventLength] {
    final def apply(c: HCursor): Decoder.Result[WikiEditEventLength] = {
      val oldLen = c.downField("old").as[Int] match {
          case Right(v) => Some(v)
          case Left(_) => None
        }
      val newLen = c.downField("new").as[Int] match {
          case Right(v) => Some(v)
          case Left(_) => None
        }
      Right(WikiEditEventLength(oldLen, newLen))
    }
  }
}

case class WikiEditEvent(
  meta: WikiEventMeta,
  bot: Boolean,
  comment: String,
  namespace: Int,
  wiki: String,
  minor: Boolean,
  len: WikiEditEventLength
)

object WikiEditEvent {
  implicit val encodeWikiEditEvent: Encoder[WikiEditEvent] = new Encoder[WikiEditEvent] {
    final def apply(s: WikiEditEvent): Json = Json.obj(
      ("meta", s.meta.asJson),
      ("bot", Json.fromBoolean(s.bot)),
      ("comment", Json.fromString(s.comment)),
      ("namespace", Json.fromInt(s.namespace)),
      ("wiki", Json.fromString(s.wiki)),
      ("minor", Json.fromBoolean(s.minor)),
      ("length", s.len.asJson),
    )
  }

  implicit val decodeWikiEditEvent: Decoder[WikiEditEvent] = new Decoder[WikiEditEvent] {
    final def apply(c: HCursor): Decoder.Result[WikiEditEvent] = {
      for {
        meta <- c.downField("meta").as[WikiEventMeta]
        bot <- c.downField("bot").as[Boolean]
        comment <- c.downField("comment").as[String]
        namespace <- c.downField("namespace").as[Int]
        wiki <- c.downField("wiki").as[String]
        minor <- c.downField("minor").as[Boolean]
        len <- c.downField("length").as[WikiEditEventLength]
      } yield WikiEditEvent(meta, bot, comment, namespace, wiki, minor, len)
    }
  }
}
