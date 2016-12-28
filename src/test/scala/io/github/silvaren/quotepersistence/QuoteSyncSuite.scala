package io.github.silvaren.quotepersistence

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QuoteSyncSuite extends FunSuite {

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

}
