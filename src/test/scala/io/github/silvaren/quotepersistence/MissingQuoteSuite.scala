package io.github.silvaren.quotepersistence

import io.github.silvaren.quotepersistence.MissingQuote.YearMonth
import org.joda.time.DateTimeConstants
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MissingQuoteSuite extends FunSuite {

  test("computes missing years") {
    val missingDates = MissingQuote.missingDateMap(Util.buildDate(2015, 12, 30), Util.buildDate(2016, 8, 18))

    assert(missingDates.years === Set(2015,2016))
  }

  test("computes missing months") {
    val missingDates = MissingQuote.missingDateMap(Util.buildDate(2015, 12, 30), Util.buildDate(2016, 8, 18))

    assert(missingDates.months === Set(YearMonth(2015,12),YearMonth(2016,1),YearMonth(2016,2),YearMonth(2016,3),
      YearMonth(2016,4),YearMonth(2016,5),YearMonth(2016,6),YearMonth(2016,7),YearMonth(2016,8)))
  }

  test("only includes weekdays") {
    val missingDates = MissingQuote.missingDateMap(Util.buildDate(2015, 12, 30), Util.buildDate(2016, 8, 18))

    assert(!missingDates.days.exists(d => d.dayOfWeek().get() === DateTimeConstants.SATURDAY ||
      d.dayOfWeek().get() === DateTimeConstants.SUNDAY))
  }

}
