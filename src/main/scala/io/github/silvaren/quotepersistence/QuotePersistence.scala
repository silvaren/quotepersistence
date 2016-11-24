package io.github.silvaren.quotepersistence

import io.github.silvaren.quoteparser.Quote
import org.mongodb.scala.{MongoClient, MongoDatabase}

object QuotePersistence {

  def persist(quotes: List[Quote]): Unit = {
    val mongoClient: MongoClient = MongoClient()

    val database = mongoClient.getDatabase("quotes")

    mongoClient.close()
  }

  def main(args: Array[String]): Unit = {
    persist(List())
  }

}
