// Este script lee el archivo csv del dataset de distritos y crea un nodo por cada distrito
// con su información relevante, no crea aristas por ahora

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

val bufferedSource = scala.io.Source.fromFile("Distritos_de_Costa_Rica_noDuplicateRegs.csv")

// Creación del array inicial de nodos (alternativa)
//var vertices = Array(
//				(0L, (("NOM_DIST", "NOM_DIST"), ("COD_DIST", 0L), ("NOM_CANT", "NOM_CANT"), ("COD_CANT", 0L), ("NOM_PROV", "NOM_PROV"), ("COD_PROV", 0L)) )
//			)

var vertices = Array(
				(0L, ("NOM_DIST", 0L, "NOM_CANT", 0L, "NOM_PROV", 0L))
			)

//Array inicial de aristas (srcNode, dstNode, distance)
var edges = Array(
				Edge(0L,0L,50)
			)

// Por cada línea leída que se lee se crea el nodo distrito respectivo
for (line <- bufferedSource.getLines) {
    var cols = line.split(",").map(_.trim)

    var nom_dist = s"${cols(0)}"
    var cod_dist = s"${cols(1)}".toLong
    var nom_cant = s"${cols(2)}"
    var cod_cant = s"${cols(3)}".toLong
    var nom_prov = s"${cols(4)}" 
    var cod_prov = s"${cols(5)}".toLong
    var id = s"${cols(6)}".toLong

    // Se crea la tupla
    var tupla = (id, (nom_dist, cod_dist, nom_cant, cod_cant, nom_prov, cod_prov))
    // Tupla alternativa
    //var tupla = (id, (("NOM_DIST", nom_dist), ("COD_DIST", cod_dist), ("NOM_CANT", nom_cant), ("COD_CANT", cod_cant), ("NOM_PROV", nom_prov), ("COD_PROV", cod_prov)) )
    
    //println(tupla)
    
    // Se agrega al arreglo de nodos
    vertices = vertices :+ tupla

    // TODO: Crear aristas
    // Se deben analizar los casos de nodos:
    // - Caso 1: NSRC ni NDST no están todavía en el grafo 
    // - Caso 2: NSRC está en el grafo pero NDST no
    // - Caso 3: NSRC no está en el grafo pero NDST sí
}

// Se crean los RDDs de nodos y aristas
val vRDD = sc.parallelize(vertices) 
val eRDD = sc.parallelize(edges)  

// Prueba de tomar los primeros n nodos del RDD de nodos
println("---------------------------------- TAKE ----------------------------------")
vRDD.take(30)
println("")

// Creación del grafo con RDD de nodos y aristas
println("--------------------------- CREACIÓN DE GRAFO ----------------------------")

val graph = Graph(vRDD, eRDD)
// Impresión de los nodos
println("-------------------------- IMPRESIÓN DE NODOS ----------------------------")
graph.vertices.collect.foreach(println)

println("------------------------------ CONSULTAS ---------------------------------")

// Consulta 1: ¿Cuántos distritos hay mapeados?
println("Cantidad de distritos (cantidad de nodos)")
val numDistritos = graph.numVertices

println("Búsquedas por id")
// Consulta 2: Buscar el nodo con id = 287
//graph.vertices.filter {case (id, ((nom_dist, nom_distVal), (cod_dist, cod_distVal), (nom_cant, nom_cantVal), (cod_cant, cod_cantVal), (nom_prov, nom_provVal), (cod_prov, cod_provVal))) => id == 287 }.collect.foreach(println)
graph.vertices.filter {case (id, (nom_dist, cod_dist, nom_cant, cod_cant, nom_prov, cod_prov)) => id == 287 }.collect.foreach(println)

// Consulta 3: Buscar el nodo con id = 419, pero sin especificar tantos atributos
graph.vertices.filter {case (id, tupla) => id == 223}.collect.foreach(println)

println("Búsquedas por nombre de provincia")
// Consulta 4: Buscar los nodos cuyo canton es SAN CARLOS
//graph.vertices.filter {case (id, ((nom_dist, nom_distVal), (cod_dist, cod_distVal), (nom_cant, nom_cantVal), (cod_cant, cod_cantVal), (nom_prov, nom_provVal), (cod_prov, cod_provVal))) => nom_prov == "PUNTARENAS" }.collect.foreach(println)
graph.vertices.filter {case (id, (nom_dist, cod_dist, nom_cant, cod_cant, nom_prov, cod_prov)) => nom_prov == "PUNTARENAS" }.collect.foreach(println)


bufferedSource.close


