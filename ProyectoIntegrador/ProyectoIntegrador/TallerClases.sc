import kantan.csv.ops.toCsvInputOps
import kantan.csv.rfc
import java.io.File
import kantan.csv.generic._
import scala.collection.immutable.ListMap

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
                          director:String
                        )

val path2DataFile1 = "D:\\Movie\\movie_dataset.csv"
var dataSource = new File(path2DataFile1).readCsv[List, movie_dataset](rfc.withHeader)
dataSource.foreach(println _)
dataSource.size

val rows = dataSource.collect({ case Right(movie_dataset) => movie_dataset})
val dataSize = rows.size
rows.foreach(println _)

val rowsFailed = dataSource.collect({ case Left(excp) => excp })

/*¿Cuál es el tiempo promedio que duran las peliculas dentro
del dataset?*/
val runtimeAvg = rows.map(_.runtime.getOrElse(0.0)).sum/dataSize

/*¿Cuántas películas han dirigido cada uno de los directores
* dentro del dataset*/

ListMap(rows.groupBy(_.director)
  .map({case(k, v) => (k, v.size)})
  .toSeq.sortWith(_._2 >_._2):_*).foreach(println)

//Popularity

rows.map(_.popularity).foreach(println)