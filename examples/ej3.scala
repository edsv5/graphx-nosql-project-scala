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
				(1L, ("SFO")),
				(2L, ("ORD")),
				(3L,("DFW"))
			)
val vRDD= sc.parallelize(vertices) // Se crea el RDD de los vertices

// take(num) tries to take num elements from starting from RDD's 0th 
// partition (if you consider 0 based indexes). So the behavior of 
// take(1) and first will be identical. 

// Se toma del RDD el primer nodo
vRDD.take(1)
// Salida:
// res1: Array[(Long, String)] = Array((1,SFO))

// Nodo default
val nowhere = "nowhere"

/************************** CREACIÓN DE LAS ARISTAS **************************/

// Estructura de las aristas:
// 
//    Edge origin id → src (Long)
//    Edge destination id → dest (Long)
//    Edge Property distance → distance (Long)

val edges = Array(
				Edge(1L,2L,1800), 
				Edge(2L,3L,800), 
				Edge(3L,1L,1400)
			)

val eRDD= sc.parallelize(edges)  

// Se toma del RDD de aristas las primeras 2 aristas
eRDD.take(2)
// Salida:
// res9: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(1,2,1800), Edge(2,3,800))

/************************** CREACIÓN DEL GRAFO DE PROPIEDADES **************************/

// Para esto, se necesita un VertexRDD (vRDD), EdgeRDD (eRDD) y un nodo default (nuestro nodo "nowhere)

val graph = Graph(vRDD,eRDD, nowhere)

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



