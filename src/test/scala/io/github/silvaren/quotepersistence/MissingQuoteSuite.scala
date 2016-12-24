package io.github.silvaren.quotepersistence

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MissingQuoteSuite extends FunSuite {

  test("correctly serializes initial date") {
    val initialDate = Util.buildDate(2015,10,9)

    val serializedDate = Serialization.serializeDate(initialDate)

    assert(serializedDate == "2015-10-09T18:00:00.000-03:00")
  }

  test("year month") {
    println(MissingQuote.missingDateMap(Util.buildDate(2015, 12, 30)))
    assert(false)
  }

}
