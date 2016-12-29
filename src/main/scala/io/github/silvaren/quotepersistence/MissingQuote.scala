package io.github.silvaren.quotepersistence

import org.joda.time.{DateTime, DateTimeConstants}

import scala.annotation.tailrec

object MissingQuote {

  case class YearMonth(year: Int, month: Int)

  def isWeekDay(date: DateTime): Boolean =
    date.dayOfWeek().get() != DateTimeConstants.SATURDAY && date.dayOfWeek().get != DateTimeConstants.SUNDAY

  def rawDatesFromBeginToEnd(initialTime: DateTime, nowTime: DateTime = DateTime.now()): Seq[DateTime] = {
    @tailrec
    def iterate(date: DateTime, acc: Seq[DateTime]): Seq[DateTime] = {
      if (date isBefore nowTime)
        iterate(date.plusDays(1), acc :+ date)
      else
        acc
    }
    iterate(initialTime, Seq())
  }

  def gatherMissingDates(initialTime: DateTime, holidays: Set[DateTime],
                         nowTime: DateTime = DateTime.now()): Seq[DateTime] = {
      rawDatesFromBeginToEnd(initialTime)
      .filter(d => isWeekDay(d))
        .filter(d => !holidays.contains(d))
  }

}
