// Este script lee el archivo csv del dataset de distritos y educacion, crea un nodo por cada distrito y asigna
// sus estadisticas de educacion. En este grafo, solo importan los nodos, no las aristas

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

val bufferedSource = scala.io.Source.fromFile("Distritos_Hogares.csv")

// Estadisticas de hogares por tomar en cuenta:
// ("CODDIST", 0L) 3
// ("PROVINCIA", "") 4
// ("CANTON", "") 5
// ("DISTRITO", "") 6
// ("TotalViviendasIndividualesOcupadas", 0L) 7
// ("PromedioOcupantesVivienda", 0L) 8
// ("PorcentajePropias", 0L) 9
// ("PorcentajeAlquiladas", 0L) 10
// ("PorcentajeBuenEstado", 0L) 11
// ("PorcentajeHacinadas", 0L) 12

// Estadísticas en el dataset:
// 0: OBJECTID_12,
// 1: CODPROV,
// 2: CODCANT,
// 3: CODDIST,
// 4: PROVINCIA,
// 5: CANTON,
// 6: DISTRITO,
// 7: Total de viviendas individuales ocupadas,
// 8: Promedio de ocupantes por vivienda,
// 9: Porcentaje Propias,
// 10: Porcentaje Alquiladas,
// 11: Porcentaje En buen estado,
// 12: Porcentaje Hacinadas

// El CODDIST va a ser la llave en estos nodos
var vertices = Array(
				(0L, (
                        ("PROVINCIA", ""),
                        ("CANTON", ""),
                        ("DISTRITO", ""),
                        ("TotalViviendasIndividualesOcupadas", 0D),
                        ("PromedioOcupantesVivienda", 0D),
                        ("PorcentajePropias", 0D),
                        ("PorcentajeAlquiladas", 0D),
                        ("PorcentajeBuenEstado", 0D),
                        ("PorcentajeHacinadas", 0D)
                    )
                )
			)

//Array inicial de aristas
var edges = Array(
				Edge(0L,0L,0)
			)

// Estadisticas de hogares por tomar en cuenta:
// ("CODDIST", 0L) 3
// ("PROVINCIA", "") 4
// ("CANTON", "") 5
// ("DISTRITO", "") 6
// ("TotalViviendasIndividualesOcupadas", 0L) 7
// ("PromedioOcupantesVivienda", 0L) 8
// ("PorcentajePropias", 0L) 9
// ("PorcentajeAlquiladas", 0L) 10
// ("PorcentajeBuenEstado", 0L) 11
// ("PorcentajeHacinadas", 0L) 12

// Por cada línea leída que se lee se crea el nodo distrito respectivo
for (line <- bufferedSource.getLines) {
    var cols = line.split(",").map(_.trim)

    var id = s"${cols(3)}".toLong
    var provincia = s"${cols(4)}"
    var canton = s"${cols(5)}"
    var distrito = s"${cols(6)}"
    var totalViviendasIndivOcupadas = s"${cols(7)}".toDouble
    var promOcupantesVivienda = s"${cols(8)}".toDouble
    var prcPropias = s"${cols(9)}".toDouble
    var prcAlquiladas = s"${cols(10)}".toDouble
    var prcBuenEstado = s"${cols(11)}".toDouble
    var prcHacinadas = s"${cols(12)}".toDouble

    // Se crea la tupla
    var tupla = (id,    
                    (
                        ("PROVINCIA", provincia),
                        ("CANTON", canton),
                        ("DISTRITO", distrito),
                        ("TOTAL_VIVIENDAS_INDIV_OCUPADAS", totalViviendasIndivOcupadas),
                        ("PROM_OCUPANTES_VIVIENDA", promOcupantesVivienda),
                        ("PRC_PROPIAS", prcPropias),
                        ("PRC_ALQUILADAS", prcAlquiladas),
                        ("PRC_BUEN_ESTADO", prcBuenEstado),
                        ("PRC_HACINADAS", prcHacinadas)
                    )
                    
                )

    // Tupla alternativa
    //var tupla = (id, (("NOM_DIST", nom_dist), ("COD_DIST", cod_dist), ("NOM_CANT", nom_cant), ("COD_CANT", cod_cant), ("NOM_PROV", nom_prov), ("COD_PROV", cod_prov)) )
    
    //println(tupla)
    
    // Se agrega al arreglo de nodos
    vertices = vertices :+ tupla
}

// Se crean los RDDs de nodos y aristas
val vRDD = sc.parallelize(vertices) 
val eRDD = sc.parallelize(edges)  

// Creación del grafo con RDD de nodos y aristas
println("--------------------------- CREACIÓN DE GRAFO ----------------------------")

val graph = Graph(vRDD, eRDD)
// Impresión de los nodos
println("-------------------------- IMPRESIÓN DE NODOS ----------------------------")
graph.vertices.collect.foreach(println)

//println("------------------------------ CONSULTAS ---------------------------------")

// Consulta 1: ¿Cuántos distritos hay mapeados?
println("Cantidad de distritos (cantidad de nodos)")
val numDistritos = graph.numVertices

//println("Búsquedas por id")
// Consulta 2: Buscar el nodo con id = 287
//graph.vertices.filter {case (id, ((nom_dist, nom_distVal), (cod_dist, cod_distVal), (nom_cant, nom_cantVal), (cod_cant, cod_cantVal), (nom_prov, nom_provVal), (cod_prov, cod_provVal))) => id == 287 }.collect.foreach(println)
//graph.vertices.filter {case (id, (nom_dist, cod_dist, nom_cant, cod_cant, nom_prov, cod_prov)) => id == 287 }.collect.foreach(println)

// Consulta 3: Buscar el nodo con id = 419, pero sin especificar tantos atributos
//graph.vertices.filter {case (id, tupla) => id == 223}.collect.foreach(println)

//println("Búsquedas por nombre de provincia")
// Consulta 4: Buscar los nodos cuyo canton es SAN CARLOS
//graph.vertices.filter {case (id, ((nom_dist, nom_distVal), (cod_dist, cod_distVal), (nom_cant, nom_cantVal), (cod_cant, cod_cantVal), (nom_prov, nom_provVal), (cod_prov, cod_provVal))) => nom_prov == "PUNTARENAS" }.collect.foreach(println)
//graph.vertices.filter {case (id, (nom_dist, cod_dist, nom_cant, cod_cant, nom_prov, cod_prov)) => nom_prov == "PUNTARENAS" }.collect.foreach(println)

bufferedSource.close


