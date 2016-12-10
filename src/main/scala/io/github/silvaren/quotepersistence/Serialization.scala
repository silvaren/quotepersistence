package io.github.silvaren.quotepersistence

import com.fatboyindustrial.gsonjodatime.Converters
import com.google.gson.{Gson, GsonBuilder, TypeAdapter}
import com.google.gson.stream.{JsonReader, JsonWriter}
import io.github.silvaren.quoteparser.{OptionQuote, Quote, StockQuote}
import io.github.silvaren.quotepersistence.QuotePersistence.MongoDocument
import org.joda.time.DateTime
import org.mongodb.scala.bson.collection.immutable.Document

object Serialization {

  private[this] lazy val gson: Gson = {
    val gsonBuilder = Converters.registerDateTime(new GsonBuilder())
    val bigDecimalAdapter = new TypeAdapter[BigDecimal] {
      override def write(out: JsonWriter, value: BigDecimal): Unit = {
        out.jsonValue(value.toString)
      }
      override def read(in: JsonReader): BigDecimal = BigDecimal(in.nextString())
    }
    gsonBuilder.registerTypeAdapter(classOf[BigDecimal], bigDecimalAdapter).create()
  }

  def serializeDate(date: DateTime): String =
    gson.toJson(date).replace("\"", "")

  def mapToQuoteObj(result: Document): Quote =
    if (result.contains("exerciseDate"))
      gson.fromJson(result.toJson(), classOf[OptionQuote])
    else
      gson.fromJson(result.toJson(), classOf[StockQuote])

  def serializeQuotesAsMongoDocuments(quotes: => Stream[Quote]): Stream[MongoDocument] = {
    quotes.map(q => Document(gson.toJson(q)))
  }

}
