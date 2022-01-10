import kantan.csv.ops.toCsvInputOps
import kantan.csv.rfc
import java.io.File

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
                          runtime: Int,
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

