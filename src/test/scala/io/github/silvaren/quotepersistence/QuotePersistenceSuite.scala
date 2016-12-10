package io.github.silvaren.quotepersistence

import de.flapdoodle.embed.mongo.config.{MongodConfigBuilder, Net}
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.{MongodExecutable, MongodProcess, MongodStarter}
import de.flapdoodle.embed.process.runtime.Network
import io.github.silvaren.quoteparser.{OptionQuote, StockQuote}
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.collection.immutable.Document
import org.scalatest.{FunSuite, Outcome}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QuotePersistenceSuite extends FunSuite {

  val StockQuoteSample = StockQuote("PETR4", buildDate(2015, 1, 2), BigDecimal("9.99"), BigDecimal("9.99"),
    BigDecimal("9.36"), BigDecimal("9.36"), 48837200, 39738)

  val OptionQuoteSample = OptionQuote("PETRA10", buildDate(2015, 1, 8), BigDecimal("0.57"), BigDecimal("0.97"),
    BigDecimal("0.50"), BigDecimal("0.82"), 44492200, 14291, BigDecimal("8.61"), buildDate(2015, 1, 19))

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

  private[this] val starter = MongodStarter.getDefaultInstance()

  var _mongodExe: Option[MongodExecutable] = None
  var _mongod: Option[MongodProcess] = None
  var _mongo: Option[MongoClient] = None

  override def withFixture(test: NoArgTest): Outcome = {
    _mongodExe = Some(starter.prepare(new MongodConfigBuilder()
      .version(Version.Main.PRODUCTION)
      .net(new Net(12345, Network.localhostIsIPv6()))
      .build()))
    _mongod = _mongodExe.map(_.start())

    _mongo = Some(MongoClient())

    try {
      test() // invoke the test function
    }
    finally {
      _mongod.foreach(_.stop())
      _mongodExe.foreach(_.stop())
    }
  }

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

    val mongoDocs = Serialization.serializeQuotesAsMongoDocuments(Stream(StockQuoteSample, OptionQuoteSample))

    assert(mongoDocs == expectedMongoDocs)
  }

  test("correctly serializes initial date") {
    val initialDate = buildDate(2015,10,9)

    val serializedDate = Serialization.serializeDate(initialDate)

    assert(serializedDate == "2015-10-09T18:00:00.000-03:00")
  }

}
