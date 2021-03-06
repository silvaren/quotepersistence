package io.github.silvaren.quotepersistence

import io.github.silvaren.quoteparser.{OptionQuote, StockQuote}
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.mongodb.scala.bson.collection.immutable.Document
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QuoteSerializationSuite extends FunSuite {

  test("persist correctly serializes quotes as mongo documents") {
    val stockQuoteJson = """
                           {
                           "symbol":"PETR4",
                           "date":"2015-01-02T18:00:00.000-02:00",
                           "openPrice":9.99,
                           "highPrice":9.99,
                           "lowPrice":9.36,
                           "closePrice":9.36,
                           "tradedVolume":48837200,
                           "trades":39738
                           }
                         """
    val optionQuoteJson = """
                           {
                           "symbol":"PETRA10",
                           "date":"2015-01-08T18:00:00.000-02:00",
                           "openPrice":0.57,
                           "highPrice":0.97,
                           "lowPrice":0.50,
                           "closePrice":0.82,
                           "tradedVolume":44492200,
                           "trades":14291,
                           "strikePrice":8.61,
                           "exerciseDate":"2015-01-19T18:00:00.000-02:00"}
                          """
    val expectedMongoDocs = List(Document(stockQuoteJson), Document(optionQuoteJson))

    val mongoDocs = Serialization.serializeQuotesAsMongoDocuments(Stream(Util.StockQuoteSample, Util.OptionQuoteSample))

    assert(mongoDocs == expectedMongoDocs)
  }

  test("correctly serializes initial date") {
    val initialDate = Util.buildDate(2015,10,9)

    val serializedDate = Serialization.serializeDate(initialDate)

    assert(serializedDate == "2015-10-09T18:00:00.000-03:00")
  }

}
