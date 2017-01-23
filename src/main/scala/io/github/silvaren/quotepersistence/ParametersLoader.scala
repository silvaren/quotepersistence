package io.github.silvaren.quotepersistence

import com.google.gson.GsonBuilder

object ParametersLoader {

  final case class DbConfig(var port: Int, var dbName: String, var collection: String) {
    def this() = this(0, "", "")
  }

  case class Parameters(var quoteDir: String, var dbConfig: DbConfig, var selectedMarkets: Array[Int],
                              var selectedSymbols: Array[String], baseUrl: String, fileNamePrefix: String) {
    def this() = this("", new DbConfig(0, "", ""), new Array[Int](0), new Array[String](0), "", "")
  }

  def loadParametersFile(filePath: String): Parameters = {
    val source = scala.io.Source.fromFile(filePath)
    val lines = try source.mkString finally source.close()
    val gson = new GsonBuilder().create()
    gson.fromJson(lines, classOf[Parameters])
  }

}
