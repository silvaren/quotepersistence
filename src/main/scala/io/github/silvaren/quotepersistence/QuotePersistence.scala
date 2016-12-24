package io.github.silvaren.quotepersistence

import io.github.silvaren.quoteparser.Quote
import io.github.silvaren.quotepersistence.ParametersLoader.DbConfig
import org.joda.time.DateTime
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.{IndexOptions, Indexes}
import org.mongodb.scala.{Completed, MongoClient, MongoCollection, Observer}

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global

object QuotePersistence {

  val BatchSize: Int = 1000
  type MongoDocument = org.mongodb.scala.Document

  case class QuoteDb(mongoClient: MongoClient, collection: MongoCollection[MongoDocument])

  def connectToQuoteDb(dbConfig: DbConfig): Future[QuoteDb] = {
    val mongoClient: MongoClient = MongoClient(s"mongodb://localhost:${dbConfig.port}")
    val database = mongoClient.getDatabase(dbConfig.dbName)
    val dbCollection = database.getCollection(dbConfig.collection)
    val indexP = Promise[String]
    dbCollection.createIndex(Indexes.ascending("symbol","date"), new IndexOptions().unique(true)).subscribe(
      new Observer[String] {
        override def onError(e: Throwable) = indexP.failure(e)
        override def onComplete() = indexP.success("Index completed!")
        override def onNext(result: String) = println(result)
      })
    indexP.future.map(_ => QuoteDb(mongoClient, dbCollection))
  }

  def disconnectFromQuoteDb(quoteDb: QuoteDb): Unit = quoteDb.mongoClient.close()

  def insertQuoteStreamSequence(quoteSeqs: Seq[Stream[Quote]], quoteDb: QuoteDb): Future[Seq[String]]=
    Future.sequence(quoteSeqs.flatMap(quotes => QuotePersistence.insertQuotes(quotes, quoteDb)))

  def insertQuotes(quotes: => Stream[Quote], quoteDb: QuoteDb): Seq[Future[String]] = {
    persistToDatabaseCollection(quotes, quoteDb.collection)
  }

  def lastQuoteDate(symbol: String, quoteDb: QuoteDb): Future[DateTime] = {
    val lastQuoteQueryP = Promise[Seq[Quote]]()
    val lastQuotes = scala.collection.mutable.ListBuffer[Quote]() // breaking immutability :(
    quoteDb.collection.find(equal("symbol", symbol))subscribe (
      new Observer[Document] {
        override def onNext(result: Document): Unit = lastQuotes += Serialization.mapToQuoteObj(result)
        override def onError(e: Throwable): Unit = lastQuoteQueryP.failure(e)
        override def onComplete(): Unit = lastQuoteQueryP.success(lastQuotes.toSeq)
      })
    implicit def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)
    val sortedQuotes = lastQuoteQueryP.future.map( quotes => quotes.sortBy(_.date))
    sortedQuotes.map( quotes => quotes.last.date)
  }

  def findQuotesFromInitialDate(symbol: String, initialDate: DateTime, quoteDb: QuoteDb): Future[Seq[Quote]] = {
    val quoteQueryP = Promise[Seq[Quote]]()
    val quoteSeq = scala.collection.mutable.ListBuffer[Quote]() // breaking immutability :(
    quoteDb.collection.find(and(equal("symbol", symbol),gt("date",
      Serialization.serializeDate(initialDate)))).subscribe (
      new Observer[Document] {
        override def onNext(result: Document): Unit = quoteSeq += Serialization.mapToQuoteObj(result)
        override def onError(e: Throwable): Unit = quoteQueryP.failure(e)
        override def onComplete(): Unit = quoteQueryP.success(quoteSeq.toSeq)
      })
    quoteQueryP.future
  }

  def retrieveUpdatedQuotes(symbol: String, initialDate: DateTime, quoteDb: QuoteDb): Future[Seq[Quote]] = {
    lastQuoteDate(symbol, quoteDb).flatMap( date => {
      println(date)
      println(MissingQuote.missingDateMap(date))
      findQuotesFromInitialDate(symbol, initialDate, quoteDb)
    })
  }

  private[this] def insertInBatches(quoteDocs: => Stream[Document], dbCollection: MongoCollection[MongoDocument],
                                    acc: Seq[Future[String]]): Seq[Future[String]] = {
    val quoteDocsPiece = quoteDocs.take(BatchSize)
    if (quoteDocsPiece.size > 0) {
      val p = Promise[String]()
      dbCollection.insertMany(quoteDocsPiece).subscribe(new Observer[Completed] {
        override def onNext(result: Completed): Unit = println("Inserted")
        override def onError(e: Throwable): Unit = p.failure(e)
        override def onComplete(): Unit = p.success("Success!")
      })
      insertInBatches(quoteDocs.drop(BatchSize), dbCollection, p.future +: acc)
    } else
      acc
  }

  private[this] def persistToDatabaseCollection(quotes: => Stream[Quote], dbCollection: MongoCollection[MongoDocument]):
  Seq[Future[String]] = {
    def quoteDocs = Serialization.serializeQuotesAsMongoDocuments(quotes)
    insertInBatches(quoteDocs, dbCollection, Seq())
  }
}
