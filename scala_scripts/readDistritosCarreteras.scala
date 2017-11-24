
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
// import org.apache.spark.io._ 

val fileCarreteras = scala.io.Source.fromFile("testDataset_noExtraComas_noEmptyRegs_SrcDstAdded.csv")
println("+--------------------------------------------------------+")
println("| OBJECTID_1, OBJECTID, RL, TIPO, NSRC, NDST, SHAPE_Leng |")
println("+--------------------------------------------------------+")

for (line <- fileCarreteras.getLines) {

    val cols = line.split(",").map(_.trim)
    //val colsNext = bufferedSource.getLines.next().split(",").map(_.trim)

    val objId1 = s"${cols(0)}"
    val objId = s"${cols(1)}"
    val rl = s"${cols(2)}"
    val tipo = s"${cols(3)}"
    val nsrc = s"${cols(4)}" 
    val ndst = s"${cols(5)}"
    val shape_lenght = s"${cols(6)}"
    
    // val Array(month, revenue, expenses, profit) = line.split(",").map(_.trim)
    println("OBJECTID_1: " + objId1 + " | OBJECTID: " + objId + " | RL: " + rl + " | TIPO: " + tipo, " | NSRC: " + nsrc + " | NDST: " + ndst + " | SHAPE_Leng: " +  shape_lenght)
}
bufferedSource.close


