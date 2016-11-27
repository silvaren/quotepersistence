package io.github.silvaren.quotepersistence

import com.fatboyindustrial.gsonjodatime.Converters
import com.google.gson.stream.{JsonReader, JsonWriter}
import com.google.gson.{Gson, GsonBuilder, TypeAdapter}
import io.github.silvaren.quoteparser.Quote
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.{Completed, MongoClient, MongoCollection, Observer}

object QuotePersistence {

  type MongoDocument = org.mongodb.scala.Document

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


  def persist(quotes: List[Quote], dbName: String, collection: String, insertCallback: Observer[Completed]): Unit = {
    val mongoClient: MongoClient = MongoClient()
    val database = mongoClient.getDatabase(dbName)
    val dbCollection = database.getCollection(collection)

    persistToDatabaseCollection(quotes, dbCollection, insertCallback)

    mongoClient.close()
  }

  def persistToDatabaseCollection(quotes: List[Quote], dbCollection: MongoCollection[MongoDocument],
                                  insertCallback: Observer[Completed]): Unit = {
    val quoteDocs: List[MongoDocument] = serializeQuotesAsMongoDocuments(quotes)
    dbCollection.insertMany(quoteDocs).subscribe(insertCallback)
  }

  def serializeQuotesAsMongoDocuments(quotes: List[Quote]): List[MongoDocument] = {
    val gson = createGson()
    quotes.map(q => Document(gson.toJson(q)))
  }
}
