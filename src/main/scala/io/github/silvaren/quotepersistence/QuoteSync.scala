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

  def insertRemoteQuoteFile(fileName: String, quoteDb: QuoteDb, parameters: Parameters): Future[Seq[String]] =
    downloadQuoteFile(parameters.baseUrl, parameters.quoteDir, fileName)
      .map(is => QuoteParser.parse(is, parameters.selectedMarkets.toSet, parameters.selectedSymbols.toSet))
      .flatMap(quoteStream => Future.sequence(QuotePersistence.insertQuotes(quoteStream, quoteDb)))

  def retrieveUpdatedQuotes(symbol: String, initialDate: DateTime, quoteDb: QuoteDb,
                            parameters: Parameters): Future[Seq[Quote]] = {
    QuotePersistence.lastQuoteDate(symbol, quoteDb).flatMap( lastPersistedDate => {
      assert(previousYearIsPreloaded(lastPersistedDate))
      val missingDates = MissingQuote.gatherMissingDates(lastPersistedDate.plusDays(1))
      val insertRemoteQuotesFuture: Future[Seq[String]] = {
        if (shouldDownloadAnnualFile(missingDates)) {
          val annualFileName = annualFileNameForYear(DateTime.now().year().get(), parameters.fileNamePrefix)
          insertRemoteQuoteFile(annualFileName, quoteDb, parameters)
        } else {
          val fileNames = missingDates.days
            .map(d => dailyFileNameForYear(d.year().get(), d.monthOfYear().get(),
              d.dayOfMonth().get(), parameters.fileNamePrefix))
          // TODO: filter out weekends and holidays
          Future.sequence(fileNames.map(fileName => insertRemoteQuoteFile(fileName, quoteDb, parameters)))
            .map(nestedSeq => nestedSeq.flatten)
        }
      }
      insertRemoteQuotesFuture.flatMap( _ => QuotePersistence.findQuotesFromInitialDate(symbol, initialDate, quoteDb))
    })
  }

}
