package io.github.silvaren.quotepersistence

import java.io.{File, FileInputStream}

import io.github.silvaren.quoteparser.QuoteParser
import io.github.silvaren.quotepersistence.ParametersLoader.Parameters

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object FileScanner {

  private[this] def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  private[this] def parseAllFiles(parameters: Parameters): Unit = {
    val fileList = getListOfFiles(parameters.quoteDir)
    val fStreams = fileList.map(f => new FileInputStream(f))
    def quoteSeqs = fStreams.map(fStream => QuoteParser.parse(fStream, parameters.selectedMarkets.toSet,
      parameters.selectedSymbols.toSet))
    val quotePersistence = QuotePersistenceFactory.connectToQuoteDb(parameters.dbConfig)
    val insertFutures = quotePersistence.map(
      persistence => quoteSeqs.flatMap(quotes => persistence.insertQuotes(quotes)))
    val insertFutureSequence = insertFutures.flatMap(insertFs => Future.sequence(insertFs))
    val disconnectF = insertFutureSequence.map(
      _ => quotePersistence.foreach(persistence => persistence.disconnectFromQuoteDb()))
    Await.result(disconnectF, Duration.Inf)
  }

  def main(args: Array[String]): Unit = {
    val parameters = ParametersLoader.loadParametersFile(args(0))
    parseAllFiles(parameters)
  }

}
