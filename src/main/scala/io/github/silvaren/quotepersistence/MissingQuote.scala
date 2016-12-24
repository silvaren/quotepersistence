package io.github.silvaren.quotepersistence

import org.joda.time.DateTime

object MissingQuote {

  case class YearMonth(year: Int, month: Int)

  def missingDateMap(initialTime: DateTime) = {
    def iterate(date: DateTime, acc: Seq[DateTime]): Seq[DateTime] = {
      if (date isBefore DateTime.now)
        iterate(date.plusDays(1), acc :+ date)
      else
        acc
    }
    val missingDays = iterate(initialTime, Seq())
    val missingMonths = missingDays.foldLeft(Set[YearMonth]())(
      (acc, day) => acc + YearMonth(day.year().get(), day.monthOfYear().get()))
    val missingYears = missingMonths.foldLeft(Set[Int]())((acc, yearMonth) => acc + yearMonth.year)
    (missingYears, missingMonths, missingDays)
  }

}
