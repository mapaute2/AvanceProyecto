import kantan.csv.ops.toCsvInputOps
import kantan.csv.rfc
import java.io.File
//Las direcciones de los path2DataFile varia en cada usuario
//Csv de movies, sin incluir las fechas, por que estan en tipo Fecha

val path2DataFile1 = "C:\\Users\\0zzda\\Downloads\\movies - Hoja 1.csv"
val dataSource1 = new File(path2DataFile1).readCsv[List, (Int, Int, String, String, Int,String, String, String,String,
  String, Double, String, String, String, Double, Int, String, String)](rfc.withHeader(true))
case class hoja1(index: String, budget: Int, genres: String, homepage: String, id: Int, keywords: String, original_language: String,
                 original_title: String,overview: String, popularity: String, runtime: Double, status: String, tagline:String,
                 title: String, vote_average: Double, vote_count: Int, cast: String, director: String)
val values1= dataSource1.collect({case Right(hoja1)=>hoja1})
//values1.foreach(println _)

//csv de production_companies como es json lo ponemos string

val path2DataFile2 = "C:\\Users\\0zzda\\Downloads\\production_companies - Hoja 1.csv"
val dataSource2 = new File(path2DataFile2).readCsv[List, (String)](rfc.withHeader(true))
case class hoja2(production_companies: String)
val values2= dataSource2.collect({case Right(hoja2)=>hoja2})
//values2.foreach(println _)

//csv de production_countries como es json lo ponemos string

val path2DataFile3 = "C:\\Users\\0zzda\\Downloads\\production_countries - Hoja 1.csv"
val dataSource3 = new File(path2DataFile3).readCsv[List, (String)](rfc.withHeader(true))
case class hoja3(production_countries: String)
val values3= dataSource3.collect({case Right(hoja3)=>hoja3})
//values3.foreach(println _)

//csv de spoken_languages como es json lo ponemos string

val path2DataFile4 = "C:\\Users\\0zzda\\Downloads\\spoken_languages - Hoja 1.csv"
val dataSource4 = new File(path2DataFile4).readCsv[List, (String)](rfc.withHeader(true))
case class hoja4(spoken_languages: String)
val values4= dataSource4.collect({case Right(hoja4)=>hoja4})
//values4.foreach(println _)

//csv de crew como es json lo ponemos string

val path2DataFile5 = "C:\\Users\\0zzda\\Downloads\\crew - Hoja 1.csv"
val dataSource5 = new File(path2DataFile5).readCsv[List, (String)](rfc.withHeader(true))
case class hoja5(crew: String)
val values5= dataSource5.collect({case Right(hoja5)=>hoja5})
//values5.foreach(println _)
