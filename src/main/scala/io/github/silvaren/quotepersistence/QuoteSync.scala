package io.github.silvaren.quotepersistence

import java.io.{File, FileInputStream, FileOutputStream, InputStream}
import java.net.URL
import java.nio.channels.Channels

import io.github.silvaren.quoteparser.{Quote, QuoteParser}
import io.github.silvaren.quotepersistence.ParametersLoader.Parameters
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

  def shouldDownloadAnnualFile(missingDates: Seq[DateTime]): Boolean = missingDates.size > 30

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

  def insertRemoteQuoteFile(fileName: String, parameters: Parameters,
                            quotePersistence: QuotePersistence): Future[Seq[String]] =
    downloadQuoteFile(parameters.baseUrl, parameters.quoteDir, fileName)
      .map(is => QuoteParser.parse(is, parameters.selectedMarkets.toSet, parameters.selectedSymbols.toSet))
      .flatMap(quoteStream => Future.sequence(quotePersistence.insertQuotes(quoteStream)))

  def seqFutures[T, U](items: TraversableOnce[T])(yourfunction: T => Future[U]): Future[Seq[U]] = {
    items.foldLeft(Future.successful[Seq[U]](Seq())) {
      (f, item) => f.flatMap {
        x => yourfunction(item).map(_ +: x)
      }
    } map (_.reverse)
  }

  def retrieveUpdatedQuotes(symbol: String, initialDate: DateTime, parameters: Parameters,
                            quotePersistence: QuotePersistence,
                            insertQuoteFn:
                            (String, Parameters, QuotePersistence) => Future[Seq[String]] = insertRemoteQuoteFile,
                            holidays: Set[DateTime]): Future[Seq[Quote]] = {
    quotePersistence.lastQuoteDate(symbol).flatMap( lastPersistedDate => {
      assert(previousYearIsPreloaded(lastPersistedDate))
      val missingDates = MissingQuote.gatherMissingDates(lastPersistedDate.plusDays(1), holidays)
      val insertRemoteQuotesFuture: Future[Seq[Seq[String]]] = {
        if (shouldDownloadAnnualFile(missingDates)) {
          val annualFileName = annualFileNameForYear(DateTime.now().year().get(), parameters.fileNamePrefix)
          insertQuoteFn(annualFileName, parameters, quotePersistence).map(stringSeq => Seq(stringSeq))
        } else {
          val fileNames = missingDates
            .map(d => dailyFileNameForYear(d.year().get(), d.monthOfYear().get(),
              d.dayOfMonth().get(), parameters.fileNamePrefix))
          seqFutures(fileNames)(fileName => insertQuoteFn(fileName, parameters, quotePersistence))
        }
      }
      insertRemoteQuotesFuture.flatMap( _ => quotePersistence.findQuotesFromInitialDate(symbol, initialDate))
    })
  }

}
