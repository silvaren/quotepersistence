package io.github.silvaren.quotepersistence

import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.scalatest.AsyncFunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._

import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
class QuoteSyncSuite extends AsyncFunSuite with MockitoSugar {

  test("previous year preload check ok when it is preloaded") {
    assert(QuoteSync.previousYearIsPreloaded(Util.buildDate(2015, 12, 30), Util.buildDate(2016, 8, 18)))
  }

  test("previous year is preloaded check works not ok when it is not preloaded") {
    assert(!QuoteSync.previousYearIsPreloaded(Util.buildDate(2015, 12, 28), Util.buildDate(2016, 8, 18)))
  }

  test("annual file name is generated") {
    assert(QuoteSync.annualFileNameForYear(2016, "COTAHIST_") === "COTAHIST_A2016")
  }

  test("daily file name is generated") {
    assert(QuoteSync.dailyFileNameForYear(2016, 11, 18, "COTAHIST_") === "COTAHIST_D20161118")
  }

  class MockableQuotePersistence extends QuotePersistence(null)

  test("should match quote returned from persistence") {
    // given
    val initialDate = Util.buildDate(2016, 12, 15)
    val parameters = mock[ParametersLoader.Parameters]
    val persistenceMock = mock[MockableQuotePersistence]
    def insertQuoteFnMock = (x: String,y: ParametersLoader.Parameters,z: QuotePersistence) =>
      Future.successful(Seq(""))
    when(parameters.fileNamePrefix).thenReturn("bla")
    when(persistenceMock.lastQuoteDate("PETR4")).thenReturn(Future.successful(Util.buildDate(2016, 12, 29)))
    when(persistenceMock.findQuotesFromInitialDate("PETR4", initialDate)).thenReturn(Future.successful(Seq(Util.StockQuoteSample)))

    // then
    QuoteSync.retrieveUpdatedQuotes("PETR4", initialDate, parameters, persistenceMock, insertQuoteFnMock,
      Set[DateTime]()).map(quoteSeq => assert(quoteSeq == Seq(Util.StockQuoteSample)))
  }



}
