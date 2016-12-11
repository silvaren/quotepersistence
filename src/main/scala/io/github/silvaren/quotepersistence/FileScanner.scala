package io.github.silvaren.quotepersistence

import java.io.{File, FileInputStream}

import com.google.gson.GsonBuilder
import io.github.silvaren.quoteparser.QuoteParser

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object FileScanner {

  final case class DbConfig(var port: Int, var dbName: String, var collection: String) {
    def this() = this(0, "", "")
  }

  final case class Parameters(var quoteDir: String, var dbConfig: DbConfig, var selectedMarkets: Array[Int],
                              var selectedSymbols: Array[String]) {
    def this() = this("", new DbConfig(0, "", ""), new Array[Int](0), new Array[String](0))
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def parseAllFiles(parameters: Parameters): Unit = {
    val fileList = getListOfFiles(parameters.quoteDir)
    val fStreams = fileList.map(f => new FileInputStream(f))
    def quoteSeqs = fStreams.map(fStream => QuoteParser.parse(fStream, parameters.selectedMarkets.toSet,
      parameters.selectedSymbols.toSet))
    val quoteDb = QuotePersistence.connectToQuoteDb(parameters.dbConfig)
    val insertPromises = quoteSeqs.flatMap(quotes => QuotePersistence.persist(quotes, quoteDb))
    val insertFutures = insertPromises.map( p => p.future)
    val insertSequence = Future.sequence(insertFutures)
    Await.result(insertSequence, Duration.Inf)
    QuotePersistence.disconnectFromQuoteDb(quoteDb)
  }

  def main(args: Array[String]): Unit = {
    val source = scala.io.Source.fromFile(args(0))
    val lines = try source.mkString finally source.close()
    val gson = new GsonBuilder().create()
    val parameters = gson.fromJson(lines, classOf[Parameters])
    parseAllFiles(parameters)
  }

}
