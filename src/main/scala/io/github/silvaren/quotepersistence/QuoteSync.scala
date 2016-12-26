package io.github.silvaren.quotepersistence

import io.github.silvaren.quoteparser.Quote
import io.github.silvaren.quotepersistence.QuotePersistence.QuoteDb
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

object QuoteSync {

  def previousYearIsPreloaded(lastDate: DateTime, nowDate: DateTime): Boolean =
    (lastDate.year() == nowDate.year() ||
      (lastDate.year() == nowDate.plusYears(-1).year() &&
        lastDate.monthOfYear().get() == 12 && lastDate.dayOfMonth().get() >= 29))

  def retrieveUpdatedQuotes(symbol: String, initialDate: DateTime, quoteDb: QuoteDb): Future[Seq[Quote]] = {
    QuotePersistence.lastQuoteDate(symbol, quoteDb).flatMap( lastPersistedDate => {
      assert(previousYearIsPreloaded(lastPersistedDate, DateTime.now()))
      val missingDates = MissingQuote.gatherMissingDates(lastPersistedDate.plusDays(1), DateTime.now())
      QuotePersistence.findQuotesFromInitialDate(symbol, initialDate, quoteDb)
    })
  }

}
