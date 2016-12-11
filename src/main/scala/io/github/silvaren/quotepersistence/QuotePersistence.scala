package io.github.silvaren.quotepersistence

import io.github.silvaren.quoteparser.Quote
import io.github.silvaren.quotepersistence.FileScanner.DbConfig
import org.joda.time.DateTime
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.{Completed, MongoClient, MongoCollection, Observer}

import scala.concurrent.Promise

object QuotePersistence {

  val BatchSize: Int = 1000
  type MongoDocument = org.mongodb.scala.Document

  case class QuoteDb(mongoClient: MongoClient, collection: MongoCollection[MongoDocument])

  def connectToQuoteDb(dbConfig: DbConfig): QuoteDb = {
    val mongoClient: MongoClient = MongoClient(s"mongodb://localhost:${dbConfig.port}")
    val database = mongoClient.getDatabase(dbConfig.dbName)
    val dbCollection = database.getCollection(dbConfig.collection)
    QuoteDb(mongoClient, dbCollection)
  }

  def disconnectFromQuoteDb(quoteDb: QuoteDb): Unit = quoteDb.mongoClient.close()

  def insertQuotes(quotes: => Stream[Quote], quoteDb: QuoteDb): Seq[Promise[String]] = {
    persistToDatabaseCollection(quotes, quoteDb.collection)
  }

  def retrieveQuotes(symbol: String, initialDate: DateTime, quoteDb: QuoteDb): Promise[Seq[Quote]] = {
    val p = Promise[Seq[Quote]]()
    val quoteSeq = scala.collection.mutable.ListBuffer[Quote]() // breaking immutability :(
    quoteDb.collection.find(and(equal("symbol", symbol),gt("date",
      Serialization.serializeDate(initialDate)))).subscribe (
      new Observer[Document] {
        override def onNext(result: Document): Unit = quoteSeq += Serialization.mapToQuoteObj(result)
        override def onError(e: Throwable): Unit = p.failure(e)
        override def onComplete(): Unit = p.success(quoteSeq.toSeq)
      })
    p
  }

  private[this] def insertInBatches(quoteDocs: => Stream[Document], dbCollection: MongoCollection[MongoDocument],
                      acc: Seq[Promise[String]]): Seq[Promise[String]] = {
    val quoteDocsPiece = quoteDocs.take(BatchSize)
    if (quoteDocsPiece.size > 0) {
      val p = Promise[String]()
      dbCollection.insertMany(quoteDocsPiece).subscribe(new Observer[Completed] {
        override def onNext(result: Completed): Unit = println("Inserted")
        override def onError(e: Throwable): Unit = {
          println("Failed", e)
          p.failure(e)
        }
        override def onComplete(): Unit = {
          println("Completed")
          p.success("Success!")
        }
      })
      insertInBatches(quoteDocs.drop(BatchSize), dbCollection, p +: acc)
    } else
      acc
  }

  private[this] def persistToDatabaseCollection(quotes: => Stream[Quote], dbCollection: MongoCollection[MongoDocument]):
  Seq[Promise[String]] = {
    def quoteDocs = Serialization.serializeQuotesAsMongoDocuments(quotes)
    insertInBatches(quoteDocs, dbCollection, Seq())
  }
}
