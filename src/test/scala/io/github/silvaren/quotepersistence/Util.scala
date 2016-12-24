package io.github.silvaren.quotepersistence

import io.github.silvaren.quoteparser.{OptionQuote, StockQuote}
import org.joda.time.{DateTime, DateTimeZone}

object Util {

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

}
