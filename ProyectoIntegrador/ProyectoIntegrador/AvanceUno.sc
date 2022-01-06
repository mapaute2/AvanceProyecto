import kantan.csv.ops.toCsvInputOps
import kantan.csv.rfc
import java.io.File
//val path2DataFile = "C:\\Users\\0zzda\\Downloads\\ArchivosCSV/
val path2DataFile = "D:\\ProyectoIntegrador\\movie_dataset.csv"
val dataSource1 = new File(path2DataFile).readCsv[List, (Int, Int, String, String, Int,String,
  String,String,String,Double,String,String,String,Int,Int,String,String,String,String,Double,
  Int,String,String,String](rfc.withHeader(true))

dataSource1.foreach(println _)
