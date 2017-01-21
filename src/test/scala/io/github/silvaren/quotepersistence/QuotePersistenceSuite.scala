package io.github.silvaren.quotepersistence

import de.flapdoodle.embed.mongo.config.{MongodConfigBuilder, Net}
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.{MongodExecutable, MongodProcess, MongodStarter}
import de.flapdoodle.embed.process.runtime.Network
import io.github.silvaren.quotepersistence.ParametersLoader.DbConfig
import org.junit.runner.RunWith
import org.mongodb.scala.MongoClient
import org.scalatest.junit.JUnitRunner
import org.scalatest.{AsyncFunSuite, FutureOutcome}

import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class QuotePersistenceSuite extends AsyncFunSuite {

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
    val quotePersistence = QuotePersistenceFactory.connectToQuoteDb(dbConfig)
    val quoteSeqs = Seq(Stream(Util.StockQuoteSample, Util.OptionQuoteSample))
    quotePersistence.flatMap(
      persistence => persistence.insertQuoteStreamSequence(quoteSeqs).flatMap(
        _ => persistence.findQuotesFromInitialDate("PETR4", Util.buildDate(2015, 1, 1))
              .flatMap( quotes => assert(quotes == Seq(Util.StockQuoteSample)))))
  }

}
