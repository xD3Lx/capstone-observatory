package observatory

import java.nio.file.Paths
import java.time.LocalDate

import observatory.Extraction.{Station, TemperatureMeasurement}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, ShouldMatchers, WordSpec}
import org.scalatest.junit.JUnitRunner

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class ExtractionTest extends WordSpec with ShouldMatchers {

  val sparkConf = new SparkConf().setMaster("local")

  val sparkSession = SparkSession.builder().
    config(sparkConf).getOrCreate()

  import sparkSession.implicits._

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  "Extraction" should {
    "read station" in {
      val stationDf = sparkSession.read.csv(fsPath("/1975.csv")).toDF()
      val df = stationDf.mapPartitions { x: Iterator[Row] => println("in part"); Seq("del").iterator}
      df.rdd.persist().take(1999)
      Thread.sleep(100000L)
      df.take(1000)
      df.map(_.toUpperCase).take(1000)



      //      val c = Extraction.locateTemperatures(2001, "/test2.csv", "/test1.csv")
//      c.foreach(println)
    }
  }
}