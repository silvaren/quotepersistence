package io.github.silvaren.quotepersistence

import java.io.{File, FileInputStream}
import java.util
import java.util.concurrent.TimeUnit

import com.google.gson.GsonBuilder
import io.github.silvaren.quoteparser.QuoteParser
import org.mongodb.scala.{Completed, Observer}

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

object FileScanner {

  final case class Parameters (var quoteDir: String, var dbName: String, var collection: String,
                               var selectedMarkets: Array[Int]) {
    def this() = this("", "", "", new Array[Int](0))
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def parseAllFiles(dir: String, dbName: String, collection: String, selectedMarketTypes: Set[Int]): Unit = {
    val fileList = getListOfFiles(dir)
    val fStreams = fileList.map(f => new FileInputStream(f))
    def quoteSeqs = fStreams.map(fStream => QuoteParser.parse(fStream, selectedMarketTypes))

    val p = Promise[String]()
    val f = p.future
    val callback = new Observer[Completed] {
      override def onNext(result: Completed): Unit = {
        println("Inserted")
      }

      override def onError(e: Throwable): Unit = {
        println("Failed", e)
        p.failure(e)
      }

      override def onComplete(): Unit = {
        println("Completed")
        p.success("Success!")
      }
    }
    quoteSeqs.foreach( quotes => QuotePersistence.persist(quotes, dbName, collection, callback))
    f.foreach(x => println(x))
    Await.result(f, Duration(10, TimeUnit.SECONDS))
  }

  def main(args: Array[String]): Unit = {
    val source = scala.io.Source.fromFile(args(0))
    val lines = try source.mkString finally source.close()
    val gson = new GsonBuilder().create()
    val parameters = gson.fromJson(lines, classOf[Parameters])
    parseAllFiles(parameters.quoteDir, parameters.dbName, parameters.collection,
      parameters.selectedMarkets.toSet)
  }

}
