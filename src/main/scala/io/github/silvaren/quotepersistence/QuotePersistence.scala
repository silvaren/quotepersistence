package io.github.silvaren.quotepersistence

import java.util.concurrent.TimeUnit

import io.github.silvaren.quoteparser.Quote
import org.mongodb.scala.{Completed, MongoClient, Observer}
import org.mongodb.scala.bson.collection.immutable.Document

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global

object QuotePersistence {

  def persist(quotes: List[Quote]): Unit = {
    val mongoClient: MongoClient = MongoClient()

    val database = mongoClient.getDatabase("quotedb")

    val collection = database.getCollection("test")

    val doc = Document("name" -> "MongoDB", "type" -> "database",
      "count" -> 1, "info" -> Document("x" -> 203, "y" -> 102))

    val p = Promise[String]()
    val f = p.future

    val producer = Future {
      collection.insertOne(doc).subscribe(new Observer[Completed] {
        override def onNext(result: Completed): Unit = {
          println("Inserted");
        }

        override def onError(e: Throwable): Unit = {
          println("Failed", e);
          p.failure(e)
        }

        override def onComplete(): Unit = {
          println("Completed");
          p.success("Success!")
        }
      })
    }

    f.foreach(x => println(x))
    Await.result(f, Duration(10, TimeUnit.SECONDS))
    mongoClient.close()
  }

  def main(args: Array[String]): Unit = {
    persist(List())
  }

}
