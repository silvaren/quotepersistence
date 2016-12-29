package io.github.silvaren.quotepersistence

import com.google.gson.GsonBuilder

object HolidaysLoader {

  final case class Year(var year: Int, var holidays: Array[String]) {
    def this() = this(0, new Array[String](0))
  }

  final case class YearHolidays(var years: Array[Year]) {
    def this() = this(new Array[Year](0))
  }

  def loadHolidaysFile(filePath: String): YearHolidays = {
    val source = scala.io.Source.fromFile(filePath)
    val lines = try source.mkString finally source.close()
    val gson = new GsonBuilder().create()
    gson.fromJson(lines, classOf[YearHolidays])
  }

}
