// Este script lee el archivo csv del dataset (que debe ser corregido antes) y crea el
// los nodos, aristas y el grafo con el que se va a trabajar.
// Si detecta que hay dos nodos con un id identico, suma su valor de SHAPE_Leng
// al SHAPE_Leng de ese nodo

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
// import org.apache.spark.io._ 

var bufferedSource = scala.io.Source.fromFile("Carreteras_de_Costa_Rica_noExtraComas_noEmptyRegs_SrcDstAdded.csv")



// Creación del array inicial de nodos

var vertices = Array(0L,String)


//Array inicial de aristas (NSRC, NDST, DISTANCIA)
var edges = Array(
				Edge(0L,0L,0D)
			)

// Nodo default
var nowhere = "nowhere"
var idCounter = 0L
var idA = 0L
var idB = 0L

// Por cada línea leída que se lee se crean dos nodos, uno origen y otro destino
// En este caso, el id es numRuta
for (line <- bufferedSource.getLines) {

    var cols = line.split(",").map(_.trim)
    //val colsNext = bufferedSource.getLines.next().split(",").map(_.trim)
    
    // Creación de los dos nodos:
    // Nodo 1:
    // - NSRC
    //
    //val objId1 = s"${cols(0)}"
    //val objId = s"${cols(1)}"
    // Solo se necesitan estos 5 atributos, el ID es el numero de ruta
    //var id = s"${cols(2)}".toLong // El id es el número de ruta
    //var tipo = s"${cols(3)}"
    var nsrc = s"${cols(4)}" 
    var ndst = s"${cols(5)}"
    var shape_length = s"${cols(6)}".toDouble
    
    idCounter = idCounter + 1
    idA = idCounter // Guarda el id del src

    // Origen
    var origen = Array(idCounter, nsrc)

    idCounter = idCounter + 1
    idB = idCounter

    // Destino
    var destino = Array(idCounter,ndst)


    // println(tupla)
    vertices = vertices :+ origen
    vertices = vertices :+ destino

    // Creación de arista

    var arista = Edge(idA, idB, shape_length) // Por ahora usa este shape_length a pesar de que no sea el exacto
    edges = edges :+ arista
}

var vRDD = sc.parallelize(vertices)
var eRDD = sc.parallelize(edges)  

// Creación del grafo con RDD de nodos y aristas
println("--------------------------- CREACIÓN DE GRAFO ----------------------------")

var graph = Graph(vRDD, eRDD)
// Impresión de los nodos

println("-------------------------- IMPRESIÓN DE NODOS ----------------------------")
//graph.vertices.collect.foreach(println)
// Cuantos nodos?
var numDistritos = graph.numVertices 
println("Total de nodos: " + numDistritos)

println("--------------------------- Prueba de group, impresion de nodos agregado ----------------------------")
//var grouped = eRDD.map{ case (idA, idB, shape_length) => ((idA,idB),(shape_length))}.reduceByKey((x,y) => (x._3+y._3))
//grouped.collect.foreach(println)
val edgesGrouped = sc.parallelize(graph.edges.groupBy(e => (e.srcId, e.dstId)).map{case (vertex, edges) => Edge(vertex._1, vertex._2, edges.map(_.attr).sum)}.collect)
//edgesGrouped.foreach(println)
val graphGrouped = Graph(vRDD, edgesGrouped)

var numCarreteras = graphGrouped.numEdges
println("Total de carreteras: "+ numCarreteras)
graphGrouped.persist()


println("-------------------------- IMPRESIÓN DE ARISTAS ----------------------------")
// Imprime las aristas
//graph.edges.collect.foreach(println) 

// graph.vertices.filter {case (id, tupla) => id == 3L }.collect.foreach(println)

//graphGrouped.triplets.sortBy(_.attr, ascending=false).map(triplet =>
  //  	"Distance " + triplet.attr.toString + // attr: Atributo de la arista, es decir, distancia
	//	" from " + triplet.srcAttr + // srcAttr: atributo del nodo origen, en este caso, ID
	//	" to " + triplet.dstAttr + "." // dstAttr: atributo del nodo destino, en este caso ID 
	//).collect. 
	//foreach(println) // Esto para imprimir


//Vertice con mayor numero de aristas
val tmp = graphGrouped.inDegrees
tmp.take(20)
//def max(a :(VertexId, Int), b :(VertexId, Int)) :(VertexId, Int) = if (a._2 > b._2) a else b
//val maxIn = graphGrouped.inDegrees.reduce(max)
//graphGrouped.vertices.filter{ case id => id = maxIn}.collect.foreach(println)
graphGrouped.inDegrees.join(vRDD).sortBy(_._2._1).take(1)


bufferedSource.close
