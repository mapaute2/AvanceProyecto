import kantan.csv.ops.toCsvInputOps
import kantan.csv.rfc
import java.io.File
val path2DataFile = "D:\\ProyectoIntegrador\\hoja1 - Hoja 1.csv"
val dataSource1 = new File(path2DataFile).readCsv[List, (Int, Int, String, String, Int)](rfc.withHeader(true))

dataSource1.foreach(println _)

case class hoja1(
                  index: String,
                  budget: Int,
                  genres: String,
                  homepage: String,
                  id: Int
                )
val movie = new File(path2DataFile).readCsv[List, (Int, Int, String, String, Int)](rfc.withHeader(true).withCellSeparator(';'))
val values = movie.collect({ case Right (hoja) => hoja})
values.foreach(println _)