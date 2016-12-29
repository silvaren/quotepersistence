package io.github.silvaren.quotepersistence

import org.joda.time.DateTimeConstants
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MissingQuoteSuite extends FunSuite {

  test("only includes weekdays") {
    val missingDates = MissingQuote.gatherMissingDates(Util.buildDate(2015, 12, 30), Set(), Util.buildDate(2016, 8, 18))

    assert(!missingDates.exists(d => d.dayOfWeek().get() === DateTimeConstants.SATURDAY ||
      d.dayOfWeek().get() === DateTimeConstants.SUNDAY))
  }

}
