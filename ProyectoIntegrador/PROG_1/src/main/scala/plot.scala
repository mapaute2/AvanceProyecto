import jdk.internal.org.jline.utils.ExecHelper.exec
//Imports de kantan
import kantan.csv.generic._
import kantan.csv.ops.toCsvInputOps
import kantan.csv.{HeaderDecoder, rfc}
//Imports de slick
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.OracleProfile.api._
import slick.collection.heterogeneous._
import slick.sql.SqlProfile.ColumnOption.Nullable
//Imports propios de scala
import scala.language.postfixOps
import oracle.sql.CLOB
import java.io.File
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps


object plot extends App {
//Se crean las case class

  final case class MovieDb(budget: String, genres: String, homepage: String, id: String, keywords: String,
                         original_language: String, original_title: String, overview: String, popularity: String,
                         production_companies: String, production_countries: String, release_date: String, revenue: String,
                         runtime: String, spoken_languages: String, status: String, tagline: String, title: String,
                         vote_average: String, vote_count: String, cast: String, crew: String, director: String)

  final case class Crew(name: String, gender: Int, department: String, job: String, credit_id: String, id: Int, idMovie: Int,
                        index: Int)

  final case class Production_companies(name: String, id: String)

  final case class Production_countries(iso_3166_1: String, name: String)

  final case class Spoken_languages(iso_639_1: String, name: String)
  //Conexion a la base de datos
  val db = Database.forConfig("myconn")
  def exec[T](program: DBIO[T]): T = Await.result(db.run(program), 10 seconds)
  //Creacion de las tablas de datos

  class CrewTable(tag: Tag) extends Table[Crew](tag, "crew") {
    def name = column[String]("name")
    def gender = column[Int]("gender")
    def department = column[String]("department")
    def job = column[String]("job")
    def credit_id = column[String]("credit_id")
    def id = column[Int]("id")
    def idMovie=column[Int]("idMovie")
    def index = column[Int]("index")
    def * = (name, gender, department, job, credit_id, id, idMovie, index).mapTo[Crew]
  }

  class Production_companiesTable(tag: Tag) extends Table[Production_companies](tag, "production_companies") {
    def id = column[String]("id")
    def name = column[String]("name")
    def * = (id, name).mapTo[Production_companies]
  }

  class Production_countriesTable(tag: Tag) extends Table[Production_countries](tag, "production_countries") {
    def iso_3166_1 = column[String]("iso_3166_1")
    def name = column[String]("name")
    def * = (iso_3166_1, name).mapTo[Production_countries]
  }

  class Spoken_languagesTable(tag: Tag) extends Table[Spoken_languages](tag, "spoken_languages") {
    def iso_639_1 = column[String]("iso_639_1")
    def name = column[String]("name")
    def * = (iso_639_1, name).mapTo[Spoken_languages]
  }
  // Se agrega el tipo de dato CLOB por que algunos datos cuentan con una extencion muy larga para formato String
  //Se agrega Nullable por que algunos filas estan sin datos, y para que no marque error por tener datos nulos
  class MoviesTable(tag: Tag) extends Table[MovieDb](tag, "movie") {
    def index = column[String]("index", O.SqlType("CLOB"),Nullable)
    def budget = column[String]("budget", O.SqlType("CLOB"),Nullable)
    def genres = column[String]("movie_genres" , O.SqlType("CLOB"),Nullable)
    def homepage = column[String]("homepage", O.SqlType("CLOB"),Nullable)
    def idMovie = column[String]("id", O.SqlType("CLOB"),Nullable)
    def keywords = column[String]("keywords", O.SqlType("CLOB"),Nullable)
    def original_language = column[String]("original_language", O.SqlType("CLOB"),Nullable)
    def original_title = column[String]("original_title", O.SqlType("CLOB"),Nullable)
    def overview = column[String]("overview", O.SqlType("CLOB"),Nullable)
    def popularity = column[String]("popularity", O.SqlType("CLOB"),Nullable)
    def production_companies = column[String]("movie_production_companies", O.SqlType("CLOB"),Nullable)
    def production_countries = column[String]("movie_production_countries", O.SqlType("CLOB"),Nullable)
    def release_date = column[String]("release_date", O.SqlType("CLOB"),Nullable)
    def revenue = column[String]("revenue", O.SqlType("CLOB"),Nullable)
    def runtime = column[String]("runtime", O.SqlType("CLOB"),Nullable)
    def spoken_languages = column[String]("movie_spoken_languages", O.SqlType("CLOB"),Nullable)
    def status = column[String]("status", O.SqlType("CLOB"),Nullable)
    def tagline = column[String]("tagline", O.SqlType("CLOB"),Nullable)
    def title = column[String]("title", O.SqlType("CLOB"),Nullable)
    def vote_average = column[String]("vote_average", O.SqlType("CLOB"),Nullable)
    def vote_count = column[String]("vote_count", O.SqlType("CLOB"),Nullable)
    def cast = column[String]("cast", O.SqlType("CLOB"),Nullable)
    def crew = column[String]("movie_crew" , O.SqlType("CLOB"),Nullable)
    def director = column[String]("director", O.SqlType("CLOB"),Nullable)
    def * = ( index :: budget :: genres :: homepage
      :: idMovie :: keywords :: original_language :: original_title :: overview :: popularity ::  production_companies
      :: production_countries ::  release_date :: revenue :: runtime :: spoken_languages :: status :: tagline
      :: title :: vote_average :: vote_count ::  cast :: crew :: director :: HNil)
      .mapTo[MovieDb]
  }

  //lazy vals de la creacion de tablas
  lazy val movies = TableQuery[MoviesTable]
  lazy val crew = TableQuery[CrewTable]
  lazy val pCompanies = TableQuery[Production_companiesTable]
  lazy val pCountries = TableQuery[Production_countriesTable]
  lazy val spokLag = TableQuery[Spoken_languagesTable]

  //Lista de tablas
  val tables = List(crew, pCompanies, pCountries, spokLag, movies)
  tables.foreach(tbl => exec(tbl.schema.create))
  //Decodificador
  implicit val pCompDec: HeaderDecoder[Production_companies] =
    HeaderDecoder.decoder("name", "id")(Production_companies.apply _)
  implicit val pContryDec: HeaderDecoder[Production_countries] =
    HeaderDecoder.decoder("iso_3166_1", "name")(Production_countries.apply _)
  implicit val sLangDec: HeaderDecoder[Spoken_languages] =
    HeaderDecoder.decoder("iso_639_1", "name")(Spoken_languages.apply _)
  implicit val crewDeec: HeaderDecoder[Crew] = HeaderDecoder.decoder("name", "gender", "department",
    "job", "credit_id", "id", "idMovie", "index")(Crew.apply _)

  //Lectura de datos de archivos csv creados previamente en pdog_dataset

  val crewData = new File("C:\\Users\\0zzda\\Downloads\\movienuevo\\crew3.csv").readCsv[List, Crew](rfc.withHeader.withCellSeparator(','))
  val pComData = new File("C:\\Users\\0zzda\\Downloads\\movienuevo\\.csv").readCsv[List, Production_companies](rfc.withHeader.withCellSeparator(','))
  val pCtryData = new File("C:\\Users\\0zzda\\Downloads\\movienuevo\\.csv").readCsv[List, Production_countries](rfc.withHeader.withCellSeparator(','))
  var sLangData = new File("C:\\Users\\0zzda\\Downloads\\movienuevo\\.csv").readCsv[List, Spoken_languages](rfc.withHeader.withCellSeparator(','))
  var mDbData = new File ("C:\\Users\\0zzda\\Downloads\\movienuevo\\.csv").readCsv[List, MovieDb](rfc.withoutHeader.withCellSeparator(','))

  //Collect de datos almacenados de forma correcta
  val crewR: Seq[Crew] = crewData.collect({
    case Right(cr) => cr
  })
  val prodComR: Seq[Production_companies] = pComData.collect({
    case Right(prodC) => prodC
  })
  val prodCountriesR: Seq[Production_countries] = pCtryData.collect({
    case Right(prodCty) => prodCty
  })
  val spLangR: Seq[Spoken_languages] = sLangData.collect({
    case Right(sl) => sl
  })
  val mDbR: Seq[MovieDb] = mDbData.collect({
    case Right(mDb) => mDb
  })

  //Poblacion de tablas a la base de datos con las lazy val
  val query1 = DBIO.seq(
    crew ++= crewR,
    pCompanies ++= prodComR,
    pCountries ++= prodCountriesR,
    spokLag ++= spLangR,
    movies ++= mDbR)
  exec(query1)
}