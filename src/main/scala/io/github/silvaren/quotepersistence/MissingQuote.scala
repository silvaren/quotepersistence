package io.github.silvaren.quotepersistence

import org.joda.time.{DateTime, DateTimeConstants}

object MissingQuote {

  case class MissingDates(years: Set[Int], months: Set[YearMonth], days: Seq[DateTime])

  case class YearMonth(year: Int, month: Int)

  def isWeekDay(date: DateTime): Boolean =
    date.dayOfWeek().get() != DateTimeConstants.SATURDAY && date.dayOfWeek().get != DateTimeConstants.SUNDAY

  def gatherMissingDates(initialTime: DateTime, nowTime: DateTime = DateTime.now()): MissingDates = {
    def iterate(date: DateTime, acc: Seq[DateTime]): Seq[DateTime] = {
      if (date isBefore nowTime)
        iterate(date.plusDays(1), acc :+ date)
      else
        acc
    }
    val missingDays = iterate(initialTime, Seq()).filter(d => isWeekDay(d))
    val missingMonths = missingDays.foldLeft(Set[YearMonth]())(
      (acc, day) => acc + YearMonth(day.year().get(), day.monthOfYear().get()))
    val missingYears = missingMonths.foldLeft(Set[Int]())((acc, yearMonth) => acc + yearMonth.year)
    MissingDates(missingYears, missingMonths, missingDays)
  }

}
