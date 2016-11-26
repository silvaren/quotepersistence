package io.github.silvaren.quotepersistence

import java.util.concurrent.TimeUnit

import com.fatboyindustrial.gsonjodatime.Converters
import com.google.gson.stream.{JsonReader, JsonWriter}
import com.google.gson.{Gson, GsonBuilder, JsonElement, TypeAdapter}
import io.github.silvaren.quoteparser.{Quote, StockQuote}
import org.joda.time.{DateTime, DateTimeZone}
import org.mongodb.scala.{Completed, MongoClient, Observer}
import org.mongodb.scala.bson.collection.immutable.Document

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global

object QuotePersistence {

  def buildDate(year: Int, month: Int, day: Int): DateTime = {
    val d = new DateTime()
    d.withZone(DateTimeZone.forID("America/Sao_Paulo"))
      .withYear(year)
      .withMonthOfYear(month)
      .withDayOfMonth(day)
      .withHourOfDay(18)
      .withMinuteOfHour(0)
      .withSecondOfMinute(0)
      .withMillisOfSecond(0)
  }

  def persist(quotes: List[Quote]): Unit = {
    val ExpectedStockQuote = StockQuote("PETR4", buildDate(2015, 1, 2), new java.math.BigDecimal("9.99"), BigDecimal("9.99"),
      BigDecimal("9.36"), BigDecimal("9.36"), 48837200, 39738)

    val gsonBuilder = Converters.registerDateTime(new GsonBuilder())
    val bigDecimalAdapter = new TypeAdapter[BigDecimal] {
      override def write(out: JsonWriter, value: BigDecimal): Unit = {
        out.jsonValue(value.toString)
      }

      override def read(in: JsonReader): BigDecimal = BigDecimal(in.nextString())
    }
    val gson = gsonBuilder.registerTypeAdapter(classOf[BigDecimal], bigDecimalAdapter).create()
    val jsonQuote = gson.toJson(ExpectedStockQuote)
    println(jsonQuote)
    println(gson.toJson(BigDecimal("12.23")))

    println(gson.fromJson(jsonQuote, classOf[StockQuote]))
    
    val mongoClient: MongoClient = MongoClient()

    val database = mongoClient.getDatabase("quotedb")

    val collection = database.getCollection("test")

    val p = Promise[String]()
    val f = p.future

    val producer = Future {
      collection.insertOne(Document(jsonQuote)).subscribe(new Observer[Completed] {
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
