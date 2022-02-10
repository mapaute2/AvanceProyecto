import kantan.csv.ops.{toCsvInputOps, toCsvOutputOps}
import kantan.csv.rfc
import play.api.libs.json._
import play.api.libs.functional.syntax._
import java.io.File
import scala.util.{Failure, Success, Try}
import kantan.csv.generic._
import scala.collection.immutable.ListMap
import play.api.libs.json.{JsArray, JsPath, Json}
import scala.collection.immutable.Nil.foldLeft

case class movie_dataset(
                          index: Int,
                          budget: Long,
                          genres: String,
                          homepage: String,
                          idMovie: Int,
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

val rows = dataSource.collect({ case Right(movie_dataset) => movie_dataset})


//Insercion de idMovie e index que son necesarios para la base de datos
def replacement(crew : String,idMovie: Int, index: Int) : String = {
  Seq(
    "\\{'" -> "{\"",
    "': '" -> "\": \"",
    "', '" -> "\", \"",
    ", '" -> ", \"",
    "': " -> "\": ",
    "}" -> s", \"idMovie\": $idMovie}",
    "}" -> s", \"index\":$index}"
  ).foldLeft(crew){case (z, (s, r)) => z.replaceAll(s, r)}
}

val crewValues = {
  rows.map(r => Try(Json.parse(replacement(r.crew, r.idMovie, r.index))))
}

val crewValid = crewValues.collect({case Success(v) => v})
val crewError = crewValues.collect({case Failure(e) => e.getMessage})

case class Crew(
                 name: String,
                 gender: Int,
                 department: String,
                 job: String,
                 creditId: String,
                 id: Int,
                 idMovie: Int,
                 index: Int
               )
val crewList = crewValid.flatten(crewMovie => crewMovie.as[JsArray].value)
  .map(crewJS => Crew(
    (crewJS \ "name").as[String],
    (crewJS \"gender").as[Int],
    (crewJS \ "department").as[String],
    (crewJS \ "job").as[String],
    (crewJS \ "credit_id").as[String],
    (crewJS \ "id").as[Int],
    (crewJS\ "idMovie").as[Int],
    (crewJS\ "index").as[Int]
  ))

implicit val crewReads: Reads[Crew]=(
  (JsPath \ "name").read[String] and
    (JsPath \"gender").read[Int] and
    (JsPath \ "department").read[String] and
    (JsPath \ "job").read[String] and
    (JsPath \ "credit_id").read[String] and
    (JsPath \ "id").read[Int] and
    (JsPath \ "idMovie").read[Int] and
    (JsPath \ "index").read[Int]
  )(Crew.apply _)

val crewList2 = crewValid.flatten(crewMovie => crewMovie.as[List[Crew]])
val out = new File("C:\\Users\\0zzda\\Downloads\\crew3.csv")
out.writeCsv(crewList2, rfc.withHeader(ss= "name", "gender", "department","job", "credit_id", "id", "idMovie", "index"))
/*
-----------------------------------------------------------------------------------------------------------
produccion companies
*/
def replacement(production_companies : String) : String = {
  Seq(
    "\\{'" -> "{\"",
    "': '" -> "\": \"",
    "', '" -> "\", \"",
    ", '" -> ", \"",
    "': " -> "\": "
  ).foldLeft(production_companies){case (z, (s, r)) => z.replaceAll(s, r)}
}
val prodCompValues = rows.map(r=> Try(Json.parse(replacement(r.production_companies))))

val prodCompValid = prodCompValues.collect({case Success(v) => v})
val prodCompFail = prodCompValues.collect({case Failure(e) => e.getMessage})

case class Production_companies(
                                 name: String,
                                 id: Int
                               )
val prodComp_List = prodCompValid.flatten(producCom_Movie => producCom_Movie.as[JsArray].value)
  .map(production_companiesJS => Production_companies(
    (production_companiesJS \ "name").as[String],
    (production_companiesJS \ "id").as[Int]
  ))

implicit val prodCompReads: Reads[Production_companies]=(
  (JsPath \ "name").read[String] and
    (JsPath \ "id").read[Int]
  )(Production_companies.apply _)

val productCompList = prodCompValid.flatten(compMovie => compMovie.as[List[Production_companies]])
val out = new File("C:\\Users\\0zzda\\Downloads\\Production_companies.csv")
out.writeCsv(productCompList, rfc.withHeader(ss= "name","id"))
/*
----------------------------------------------------------------
produccion countries
 */
def replacement(production_countries : String) : String = {
  Seq(
    "\\{'" -> "{\"",
    "': '" -> "\": \"",
    "', '" -> "\", \"",
    ", '" -> ", \"",
    "': " -> "\": "
  ).foldLeft(production_countries){case (z, (s, r)) => z.replaceAll(s, r)}
}
val prodCountryValues = rows.map(r=> Try(Json.parse(replacement(r.production_countries))))

val prodCountryValid = prodCountryValues.collect({case Success(v) => v})
val prodCountryFail = prodCountryValues.collect({case Failure(e) => e.getMessage})

case class Production_countries(
                                 iso_3166_1: String,
                                 name: String
                               )
val prodCountry_List = prodCountryValid.flatten(producCountry_Movie => producCountry_Movie.as[JsArray].value)
  .map(production_countriesJS => Production_countries(
    (production_countriesJS \ "iso_3166_1").as[String],
    (production_countriesJS \ "name").as[String]
  ))

implicit val prodCountryReads: Reads[Production_countries]=(
  (JsPath \ "iso_3166_1").read[String] and
    (JsPath \ "name").read[String]
  )(Production_countries.apply _)

val productCountryList = prodCountryValid.flatten(countryMovie => countryMovie.as[List[Production_countries]])
val out = new File("C:\\Users\\0zzda\\Downloads\\Production_countries.csv")
out.writeCsv(productCountryList, rfc.withHeader(ss= "iso_3166_1","name"))

/*
-----------------------------------------------------------------------------
spoken languajes
*/
def replacement(spoken_languages : String) : String = {
  Seq(
    "\\{'" -> "{\"",
    "': '" -> "\": \"",
    "', '" -> "\", \"",
    ", '" -> ", \"",
    "': " -> "\": "
  ).foldLeft(spoken_languages){case (z, (s, r)) => z.replaceAll(s, r)}
}
val spoken_languagesValues = rows.map(r=> Try(Json.parse(replacement(r.spoken_languages))))

val spoken_languagesValid = spoken_languagesValues.collect({case Success(v) => v})
val spoken_languagesFail = spoken_languagesValues.collect({case Failure(e) => e.getMessage})

case class Spoken_languages(
                             iso_639_1: String,
                             name: String
                           )
val spoken_languages_List = spoken_languagesValid.flatten(spokenLan_Movie => spokenLan_Movie.as[JsArray].value)
  .map(spoken_languagesJS => Spoken_languages(
    (spoken_languagesJS \ "iso_639_1").as[String],
    (spoken_languagesJS \ "name").as[String]
  ))

implicit val spokenLangReads: Reads[Spoken_languages]=(
  (JsPath \ "iso_639_1").read[String] and
    (JsPath \ "name").read[String]
  )(Spoken_languages.apply _)

val spokenLangList = spoken_languagesValid.flatten(langMovie => langMovie.as[List[Spoken_languages]])
val out = new File("C:\\Users\\0zzda\\Downloads\\Spoken_languages.csv")
out.writeCsv(spokenLangList, rfc.withHeader(ss= "iso_639_1","name"))
/*
Creacion de movie_pCompanies
*/
def replacement(production_companies : String,idMovie : Int) : String = {
  Seq(
    "\\{'" -> "{\"",
    "': '" -> "\": \"",
    "', '" -> "\", \"",
    ", '" -> ", \"",
    "': " -> "\": ",
    "}" -> s", \"idMovie\": $idMovie}",
  ).foldLeft(production_companies){case (z, (s, r)) => z.replaceAll(s, r)}
}

val prodCompValues = rows.map(r=> Try(Json.parse(replacement(r.production_companies, r.idMovie))))

val prodCompValid = prodCompValues.collect({case Success(v) => v})
val prodCompFail = prodCompValues.collect({case Failure(e) => e.getMessage})

case class Movie_pCompanies(
                             name: String,
                             id: Int,
                             idMovie: Int
                           )
val moviespComp = prodCompValid.flatten(producCom_Movie => producCom_Movie.as[JsArray].value)
  .map(production_companiesJS => Production_companies(
    (production_companiesJS \ "name").as[String],
    (production_companiesJS \ "id").as[Int]
  ))

implicit val prodCompReads: Reads[Production_companies]=(
  (JsPath \ "name").read[String] and
    (JsPath \ "id").read[Int]
  )(Production_companies.apply _)

val productCompList = prodCompValid.flatten(compMovie => compMovie.as[List[Production_companies]])
val out = new File("C:\\Users\\0zzda\\Downloads\\movie_pCompanies.csv")
out.writeCsv(productCompList, rfc.withHeader(ss= "name","idMovie"))
}