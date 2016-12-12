package io.github.silvaren.quotepersistence

import de.flapdoodle.embed.mongo.config.{MongodConfigBuilder, Net}
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.{MongodExecutable, MongodProcess, MongodStarter}
import de.flapdoodle.embed.process.runtime.Network
import io.github.silvaren.quoteparser.{OptionQuote, StockQuote}
import io.github.silvaren.quotepersistence.ParametersLoader.DbConfig
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.mongodb.scala.MongoClient
import org.scalatest.junit.JUnitRunner
import org.scalatest.{AsyncFunSuite, FutureOutcome}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
class QuotePersistenceSuite extends AsyncFunSuite {

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

  override def withFixture(test: NoArgAsyncTest): FutureOutcome = {
    _mongodExe = Some(starter.prepare(new MongodConfigBuilder()
      .version(Version.Main.PRODUCTION)
      .net(new Net(12345, Network.localhostIsIPv6()))
      .build()))
    _mongod = _mongodExe.map(_.start())

    _mongo = Some(MongoClient("mongodb://localhost:12345"))

    complete {
      super.withFixture(test) // invoke the test function
    } lastly {
      _mongod.foreach(_.stop())
      _mongodExe.foreach(_.stop())
    }
  }

  test("inserts and retrieves quotes to mongo") {
    val dbConfig = DbConfig(12345, "quotedbtest", "test")
    val quoteDb = QuotePersistence.connectToQuoteDb(dbConfig)
    val quoteSeqs = Seq(Stream(StockQuoteSample, OptionQuoteSample))
    val insertPromises = quoteSeqs.flatMap(quotes => QuotePersistence.insertQuotes(quotes, quoteDb))
    val insertFutures = insertPromises.map( p => p.future)
    val insertSequence = Future.sequence(insertFutures)
    insertSequence.flatMap( _ => {
      println("querying...");
      QuotePersistence.retrieveQuotes("PETR4", buildDate(2015, 1, 1), quoteDb).future
    }).flatMap( quotes => assert(quotes == Seq(StockQuoteSample)))
  }

}
