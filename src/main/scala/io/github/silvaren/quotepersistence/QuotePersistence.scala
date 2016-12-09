package io.github.silvaren.quotepersistence

import com.fatboyindustrial.gsonjodatime.Converters
import com.google.gson.stream.{JsonReader, JsonWriter}
import com.google.gson.{Gson, GsonBuilder, TypeAdapter}
import io.github.silvaren.quoteparser.Quote
import io.github.silvaren.quotepersistence.FileScanner.DbConfig
import org.joda.time.DateTime
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.{Completed, MongoClient, MongoCollection, Observer}

object QuotePersistence {

  val BatchSize: Int = 1000
  type MongoDocument = org.mongodb.scala.Document

  case class QuoteDb(mongoClient: MongoClient, collection: MongoCollection[MongoDocument])

  def connectToQuoteDb(dbConfig: DbConfig): QuoteDb = {
    val mongoClient: MongoClient = MongoClient()
    val database = mongoClient.getDatabase(dbConfig.dbName)
    val dbCollection = database.getCollection(dbConfig.collection)
    QuoteDb(mongoClient, dbCollection)
  }

  def disconnectFromQuoteDb(quoteDb: QuoteDb): Unit = quoteDb.mongoClient.close()

  private[this] def createGson(): Gson = {
    val gsonBuilder = Converters.registerDateTime(new GsonBuilder())
    val bigDecimalAdapter = new TypeAdapter[BigDecimal] {
      override def write(out: JsonWriter, value: BigDecimal): Unit = {
        out.jsonValue(value.toString)
      }
      override def read(in: JsonReader): BigDecimal = BigDecimal(in.nextString())
    }
    gsonBuilder.registerTypeAdapter(classOf[BigDecimal], bigDecimalAdapter).create()
  }

  def persist(quotes: => Stream[Quote], insertCallback: Observer[Completed], quoteDb: QuoteDb): Unit = {
    persistToDatabaseCollection(quotes, quoteDb.collection, insertCallback)
  }

  def serializeDate(date: DateTime): String =
    createGson().toJson(date).replace("\"", "")

  def retrieveQuotes(symbol: String, initialDate: DateTime, quoteDb: QuoteDb) = {
    val quotes = quoteDb.collection.find(gt("date", serializeDate(initialDate))).subscribe (
      new Observer[Document] {
        override def onNext(result: Document): Unit = println(result.toJson())
        override def onError(e: Throwable): Unit = println("Failed" + e.getMessage)
        override def onComplete(): Unit = println("Completed")
      })
  }

  def insertInBatches(quoteDocs: => Stream[Document], dbCollection: MongoCollection[MongoDocument],
                      insertCallback: Observer[Completed]): Unit = {
    val quoteDocsPiece = quoteDocs.take(BatchSize)
    if (quoteDocsPiece.size > 0) {
      dbCollection.insertMany(quoteDocsPiece).subscribe(insertCallback)
      insertInBatches(quoteDocs.drop(BatchSize), dbCollection, insertCallback)
    }
  }

  def persistToDatabaseCollection(quotes: => Stream[Quote], dbCollection: MongoCollection[MongoDocument],
                                  insertCallback: Observer[Completed]): Unit = {
    def quoteDocs = serializeQuotesAsMongoDocuments(quotes)
    insertInBatches(quoteDocs, dbCollection, insertCallback)
  }

  def serializeQuotesAsMongoDocuments(quotes: => Stream[Quote]): Stream[MongoDocument] = {
    val gson = createGson()
    quotes.map(q => Document(gson.toJson(q)))
  }
}
