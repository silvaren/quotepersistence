package io.github.silvaren.quotepersistence

import java.io.{File, FileInputStream, FileOutputStream, InputStream}
import java.net.URL
import java.nio.channels.{Channels, ReadableByteChannel}
import java.util.zip.ZipInputStream

import io.github.silvaren.quoteparser.{Quote, QuoteParser}
import io.github.silvaren.quotepersistence.MissingQuote.MissingDates
import io.github.silvaren.quotepersistence.QuotePersistence.QuoteDb
import net.lingala.zip4j.core.ZipFile
import net.lingala.zip4j.exception.ZipException
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.{Codec, Source}

object QuoteSync {

  def previousYearIsPreloaded(lastDate: DateTime, nowDate: DateTime = DateTime.now()): Boolean =
    (lastDate.year() == nowDate.year() ||
      (lastDate.year() == nowDate.plusYears(-1).year() &&
        lastDate.monthOfYear().get() == 12 && lastDate.dayOfMonth().get() >= 29))

  def shouldDownloadAnnualFile(missingDates: MissingDates): Boolean = missingDates.days.size > 30

  def unzip(source: File, dst: String) {
    try {
      val zipFile = new ZipFile(source)
      zipFile.extractAll(dst)
    } catch {
      case e: ZipException => e.printStackTrace()
    }
  }

  def downloadFile(url: String, dst: String) {
    val urlObj = new URL(url)
    val rbc = Channels.newChannel(urlObj.openStream())
    val fos = new FileOutputStream(dst)
    fos.getChannel().transferFrom(rbc, 0, Long.MaxValue)
  }

  def downloadAnnualFile(year: Int, url: String, dst: String) = {
    downloadFile(url, dst)
    unzip(new File(dst), dst)
    val quoteTxtIS = new FileInputStream(dst)
    QuoteParser.parse(quoteTxtIS, Set(10,70,80), Set("PETR", "VALE")).foreach(q => println(q))
  }

  def retrieveUpdatedQuotes(symbol: String, initialDate: DateTime, quoteDb: QuoteDb): Future[Seq[Quote]] = {
    QuotePersistence.lastQuoteDate(symbol, quoteDb).flatMap( lastPersistedDate => {
      assert(previousYearIsPreloaded(lastPersistedDate))
      val missingDates = MissingQuote.gatherMissingDates(lastPersistedDate.plusDays(1))
      QuotePersistence.findQuotesFromInitialDate(symbol, initialDate, quoteDb)
    })
  }

}
