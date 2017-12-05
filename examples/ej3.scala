/** 
	Ejemplo de creación de un grafo sencillo para representar rutas entre aeropuertos.
	Primero, se crean nodos para representar cada aeropuerto, con ID y un string para su
	nombre.
	Después, se crean aristas, con origen, destino y un valor que representa su distancia
	Luego, se crea un grafo de propiedades utilizando los RDDs creados para los nodos, aristas
	y un nodo default.
	Por último se hacen algunas consultas al grafo de propiedades y se explora la clase 
	EdgeTriplet.
**/


import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

/************************** CREACIÓN DE LOS NODOS **************************/

val vertices = Array(
				(1L, ("SFO", 3, 0)),
				(2L, ("ORD", 5, 2)),
				(3L, ("DFW", 3, 1))
			)

val vRDD : RDD[(Long, (String, Int, Int))] = sc.parallelize(vertices) // Se crea el RDD de los vertices



// take(num) tries to take num elements from starting from RDD's 0th 
// partition (if you consider 0 based indexes). So the behavior of 
// take(1) and first will be identical. 

// Se toma del RDD el primer nodo
vRDD.take(1)
// Salida:
// res1: Array[(Long, String)] = Array((1,SFO))

// Nodo default
val nowhere = ("nowhere", 0, 0)

/************************** CREACIÓN DE LAS ARISTAS **************************/

// Estructura de las aristas:
// 
//    Edge origin id → src (Long)
//    Edge destination id → dest (Long)
//    Edge Property distance → distance (Long)

val edges = Array(
				Edge(1L,2L,1800),
				Edge(1L,2L,200),
				Edge(1L,2L,200),
				Edge(1L,2L,200),
				Edge(1L,2L,200),
				Edge(2L,3L,800), 
				Edge(3L,1L,1400)
			)

val eRDD : RDD[Edge[Int]] = sc.parallelize(edges)  

// Se toma del RDD de aristas las primeras 2 aristas
eRDD.take(2)
// Salida:
// res9: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(1,2,1800), Edge(2,3,800))

/************************** CREACIÓN DEL GRAFO DE PROPIEDADES **************************/

// Para esto, se necesita un VertexRDD (vRDD), EdgeRDD (eRDD) y un nodo default (nuestro nodo "nowhere)

val graph : Graph[(String, Int, Int), Int] = Graph(vRDD,eRDD, nowhere)

// Imprime los nodos
graph.vertices.collect.foreach(println) // El .foreach(println) es para que los imprima bien
// Salida:
// (1,SFO)
// (2,ORD)
// (3,DFW)

// Imprime las aristas
graph.edges.collect.foreach(println)  
// Salida:
// Edge(1,2,1800)
// Edge(2,3,800)
// Edge(3,1,1400)

/************************** CONSULTAS AL GRAFO **************************/

// Consulta 1: ¿Cuántos aeropuertos hay?

val numairports = graph.numVertices
// Salida:
// numairports: Long = 3

// Consulta 2: ¿Cuántas rutas hay?
val numroutes = graph.numEdges
// Salida:
// numroutes: Long = 3

// Consulta 3: ¿Cuántas rutas se tienen que midan menos de 1000 millas?
graph.edges.filter {case Edge(src, dst, prop) => prop > 1000 }.collect.foreach(println)
// Salida:
// Edge(1,2,1800)
// Edge(3,1,1400)

// La clase EdgeTriplet extiende la clase Edge añadiendo los miembros srcAttr y 
// dstAttr, que contienen las propiedades de origen y destino de las aristas

// Esto imprime los primeros tres triplets del grafo de propiedades
// Cada triplet contiene: ((idNodoOrigen, propiedadNodoOrigen), (idNodoDestino, propiedadNodoDestino), valorDeLaArista)
graph.triplets.take(3).foreach(println) 

// Salida:
// ((1,SFO),(2,ORD),1800)
// ((2,ORD),(3,DFW),800)
// ((3,DFW),(1,SFO),1400)

// Consulta 4: Ordenar e imprimir las rutas más largas (valor más largo de arista (ojo, esto nos sirve un montón))

graph.triplets.sortBy(_.attr, ascending=false).map(triplet =>
    	"Distance " + triplet.attr.toString + // attr: Atributo de la arista, es decir, distancia
		" from " + triplet.srcAttr + // srcAttr: atributo del nodo origen, en este caso, ID
		" to " + triplet.dstAttr + "." // dstAttr: atributo del nodo destino, en este caso ID 
	).collect. 
	foreach(println) // Esto para imprimir

// Salida:
// Distance 1800 from SFO to ORD.
// Distance 1400 from DFW to SFO.
// Distance 800 from ORD to DFW.

val newEdges = sc.parallelize(graph.edges.groupBy(e => (e.srcId, e.dstId)).map{case (vertex,edges) => Edge(vertex._1, vertex._2, edges.map(_.attr).sum)}.collect)
eRDD.take(3)
newEdges.take(3)
val newGraph = Graph(vRDD,newEdges,nowhere);
newGraph.triplets.take(3).foreach(println)

//val temp = graph.aggregateMessages[int](triplet => {triplet.sendToDst(triplet.attr)},_ + _, TripletFields.EdgeOnly).toDF("id","value")

val vertices2 = Array(
				(1L, ("SFO", 0, 1, 5)),
				(2L, ("ORD", 2, 1, 5)),
				(3L, ("DFW", 0, 0, 5))
			)
val vRDD2 : RDD[(Long, (String, Int, Int, Int))] = sc.parallelize(vertices2)
//var newVert : RDD[(Long, (String, Int, Int, Int, String, Int, Int, Int, Int))] = vRDD.join(vRDD2)
var newVert = sc.parallelize(vRDD.join(vRDD2).map{e => (e._1, (e._2._1._1,e._2._1._2,e._2._1._3,e._2._2._1,e._2._2._2,e._2._2._3,e._2._2._4))}.collect)
newVert.take(3)

val graph2 = Graph(newVert, eRDD)

val top2 = graph2.vertices.take(2)

for ((id, propiedades) <- top2) { println(s"Aeropuerto con id $id se llama ${propiedades._1}") }
