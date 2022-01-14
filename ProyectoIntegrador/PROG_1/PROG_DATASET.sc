import kantan.csv.ops.toCsvInputOps
import kantan.csv.rfc
import java.io.File
import scala.util.{Failure, Success, Try}
import kantan.csv.generic._
import scala.collection.immutable.ListMap
import play.api.libs.json.{JsArray, Json}
import scala.collection.immutable.Nil.foldLeft

case class movie_dataset(
                          index: Int,
                          budget: Long,
                          genres: String,
                          homepage: String,
                          id: Int,
                          keywords: String,
                          original_language: String,
                          original_title: String,
                          overview: String,
                          popularity: Double,
                          production_companies: String,
                          production_countries: String,
                          release_date: String,
                          revenue: Long,
                          runtime: Option[Double],
                          spoken_languages: String,
                          status: String,
                          tagline: String,
                          title: String,
                          vote_average: Double,
                          vote_count: Int,
                          cast: String,
                          crew :String,
                          director:String)

val path2DataFile1 = "C:\\Users\\0zzda\\Downloads\\movie_dataset (1).csv"
var dataSource = new File(path2DataFile1).readCsv[List, movie_dataset](rfc.withHeader)
//dataSource.foreach(println _)
val rows = dataSource.collect({ case Right(movie_dataset) => movie_dataset})

val dataSize = rows.size
rows.foreach(println _)
val rowsFailed = dataSource.collect({ case Left(excp) => excp })

val runtimeAvg = rows.map(_.runtime.getOrElse(0.0)).sum/dataSize

//

ListMap(rows.groupBy(_.director)
  .map({case(k, v) => (k, v.size)})
  .toSeq.sortWith(_._2 >_._2):_*).foreach(println)

//rows.map(_.popularity).foreach(println)
val crewTryValues = rows.map(_.crew.replace("'","\""))
  .map(data => Try(Json.parse(data)))
val crewValid = crewTryValues.collect({case Success(v)=>v})
val crewFail = crewTryValues.collect({case Failure(f)=> f.getMessage})

def replacement(crew : String) : String = {
  Seq(
    "\\{'" -> "{\"",
    "': '" -> "\": \"",
    "', '" -> "\", \"",
    ", '" -> ", \" ",
    "': " -> "\": "
  ).foldLeft(crew){case (z, (s, r)) => z.replaceAll(s, r)}
}
val crewValuesV2 = rows.map(r=> Try(Json.parse(replacement(r.crew))))
val crewValidV2 = crewValuesV2.collect({case Success(v) => v})
val crewErrorV2 = crewValuesV2.collect({case Failure(e) => e.getMessage})

crewValidV2.size
crewErrorV2.size

val prodCompList = rows
  .map(mv => Json.parse(mv.production_companies))
  .flatMap(json => (json \\ "name"))
  .map(_.as[String])
  .distinct
  .sorted
prodCompList.foreach(println)

val listJsValue = rows.map(pcomp => Json.parse(pcomp.production_companies))
  .flatten(arr => arr.as[JsArray].value)
  .map(obj =>((obj \ "name").as[String], (obj \"id").as[Int]))
listJsValue.size
