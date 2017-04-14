package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.util.Try

/**
  * 1st milestone: data extraction
  */
object Extraction {

  private val sparkConf = new SparkConf().setMaster("local")

  private val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import sparkSession.implicits._

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {
    val stationDf = sparkSession.read.csv(fsPath(stationsFile))
    val temperatureDf = sparkSession.read.csv(fsPath(temperaturesFile))
    val stations = readStations(stationDf).cache()
    val temperatures = readTemperatures(temperatureDf, year)

    temperatures.join(stations, Seq("stn", "wban")).map { r: Row =>
      val temp = r.getAs[Double]("temperature")
      val date = r.getAs[Row]("date")
      val loc = r.getAs[Row]("location")
      (LDate(date.getAs("year"), date.getAs("month"), date.getAs("day")), Location(loc.getAs("lat"), loc.getAs("lon")), temp)
    }.collect().map { case (date, loc, temp) =>
      (LocalDate.of(date.year, date.month, date.day), Location(loc.lat, loc.lon), temp)
    }
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    records.groupBy(_._2).map { case (loc, coll) => loc -> coll.map(_._3).sum / coll.size }
  }


  private def readStations(df: DataFrame): Dataset[Station] = {
    df.flatMap { r =>
      val stn = Option(r.getAs[String](0)).getOrElse("")
      val wban = Option(r.getAs[String](1)).getOrElse("")
      val latitudeOpt = Try(r.getAs[String](2).toDouble).toOption
      val longitudeOpt = Try(r.getAs[String](3).toDouble).toOption

      for {
        latitude <- latitudeOpt
        longitude <- longitudeOpt
      } yield {
        Station(stn, wban, Location(latitude, longitude))
      }
    }
  }

  private def readTemperatures(df: DataFrame, year: Int): Dataset[TemperatureMeasurement] = {
    df.flatMap { r =>
      val stn = Option(r.getAs[String](0)).getOrElse("")
      val wban = Option(r.getAs[String](1)).getOrElse("")
      val month = r.getAs[String](2).toInt
      val day = r.getAs[String](3).toInt
      val temp = r.getAs[String](4)

      if (temp != "9999.9") {
        val cel = (temp.toDouble - 32) / 1.8
        val date = LDate(year, month, day)
        Option(TemperatureMeasurement(stn, wban, date, cel))
      } else {
        None
      }
    }
  }

  private def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  case class Station(stn: String, wban: String, location: Location)

  case class TemperatureMeasurement(stn: String, wban: String, date: LDate, temperature: Double)

}
