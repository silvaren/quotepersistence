package io.github.silvaren.quotepersistence

import java.io.{File, FileInputStream, FileOutputStream, InputStream}
import java.net.URL
import java.nio.channels.Channels

import io.github.silvaren.quoteparser.{Quote, QuoteParser}
import io.github.silvaren.quotepersistence.MissingQuote.MissingDates
import io.github.silvaren.quotepersistence.ParametersLoader.Parameters
import io.github.silvaren.quotepersistence.QuotePersistence.QuoteDb
import net.lingala.zip4j.core.ZipFile
import net.lingala.zip4j.exception.ZipException
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object QuoteSync {

  def previousYearIsPreloaded(lastDate: DateTime, nowDate: DateTime = DateTime.now()): Boolean =
    lastDate.year() == nowDate.year() ||
      (lastDate.year() == nowDate.plusYears(-1).year() &&
        lastDate.monthOfYear().get() == 12 && lastDate.dayOfMonth().get() >= 29)

  def shouldDownloadAnnualFile(missingDates: MissingDates): Boolean = missingDates.days.size > 30

  def unzip(source: File, dst: String) = {
    try {
      val zipFile = new ZipFile(source)
      zipFile.extractAll(dst)
    } catch {
      case e: ZipException => e.printStackTrace()
    }
  }

  def downloadFile(url: String, dst: String) = {
    val urlObj = new URL(url)
    val rbc = Channels.newChannel(urlObj.openStream())
    val fos = new FileOutputStream(dst)
    fos.getChannel().transferFrom(rbc, 0, Long.MaxValue)
  }

  def downloadQuoteFile(baseUrl: String, quoteDir: String, fileName: String): Future[InputStream] = {
    Future {
      val url = baseUrl + fileName
      val downloadedFilePath = quoteDir + File.separator + fileName + ".ZIP"
      downloadFile(url, downloadedFilePath)
      unzip(new File(downloadedFilePath), quoteDir)
      new FileInputStream(quoteDir + File.separator + fileName + ".TXT")
    }
  }

  def annualFileNameForYear(year: Int, fileNamePrefix: String): String = fileNamePrefix + s"A$year"

  def dailyFileNameForYear(year: Int, month: Int, day: Int, fileNamePrefix: String): String =
    fileNamePrefix + s"D$year$month$day"

  def retrieveUpdatedQuotes(symbol: String, initialDate: DateTime, quoteDb: QuoteDb,
                            parameters: Parameters): Future[Seq[Quote]] = {
    QuotePersistence.lastQuoteDate(symbol, quoteDb).flatMap( lastPersistedDate => {
      assert(previousYearIsPreloaded(lastPersistedDate))
      val missingDates = MissingQuote.gatherMissingDates(lastPersistedDate.plusDays(1))
      if (shouldDownloadAnnualFile(missingDates)) {
        val annualFileName = annualFileNameForYear(DateTime.now().year().get(), parameters.fileNamePrefix)
        val downloadedIS = downloadQuoteFile(parameters.baseUrl, parameters.quoteDir, annualFileName)
          .map(is => QuoteParser.parse(is, parameters.selectedMarkets.toSet, parameters.selectedSymbols.toSet))
          .flatMap(quoteStream => Future.sequence(QuotePersistence.insertQuotes(quoteStream, quoteDb)))
      } else {
        // TODO: process missing daily files
      }
      QuotePersistence.findQuotesFromInitialDate(symbol, initialDate, quoteDb)
    })
  }

}
