package io.github.silvaren.quotepersistence

import java.io.File

import com.github.nscala_time.time.Imports._
import org.joda.time.DateTimeConstants

import scala.annotation.tailrec

object MissingQuote {

  val formatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.forID("America/Sao_Paulo"))

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

  def loadHolidaysForThisYear(year: Int = DateTime.now().year().get(), quoteDir: String): Set[DateTime] = {
    val loadedHolidays = HolidaysLoader.loadHolidaysFile(quoteDir + File.separator + "holidays.json")
    assert(loadedHolidays.years.exists(y => y.year == year))
    loadedHolidays.years.find(y => y.year == year)
      .map(y => y.holidays.toSet)
      .map(holidayStrings => holidayStrings.map(hs => formatter.parseDateTime(hs).withHour(18)))
      .get // this is not optional, we want to fail if not present
  }

}
