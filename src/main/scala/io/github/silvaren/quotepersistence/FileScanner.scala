package io.github.silvaren.quotepersistence

import java.io.{File, FileInputStream}
import java.util.concurrent.TimeUnit

import io.github.silvaren.quoteparser.QuoteParser
import org.mongodb.scala.{Completed, Observer}

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.Duration

import scala.concurrent.ExecutionContext.Implicits.global

object FileScanner {

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def parseAllFiles(dir: String, dbName: String, collection: String): Unit = {
    val fileList = getListOfFiles(dir)
    val fStreams = fileList.map(f => new FileInputStream(f))
    val quoteSeqs = fStreams.map(fStream => QuoteParser.parse(fStream))

    val p = Promise[String]()
    val f = p.future
    val callback = new Observer[Completed] {
      override def onNext(result: Completed): Unit = {
        println("Inserted");
      }

      override def onError(e: Throwable): Unit = {
        println("Failed", e);
        p.failure(e)
      }

      override def onComplete(): Unit = {
        println("Completed");
        p.success("Success!")
      }
    }
    quoteSeqs.foreach( quotes => QuotePersistence.persist(quotes, dbName, collection, callback))
    f.foreach(x => println(x))
    Await.result(f, Duration(10, TimeUnit.SECONDS))
  }

  def main(args: Array[String]): Unit = {
    parseAllFiles(args(0), args(1), args(2))
  }

}
