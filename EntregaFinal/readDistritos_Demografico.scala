// Este script lee el archivo csv del dataset de distritos y educacion, crea un nodo por cada distrito y asigna
// sus estadisticas de educacion. En este grafo, solo importan los nodos, no las aristas

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

val bufferedSource = scala.io.Source.fromFile("Distritos_Sociales_y_Demograficos.csv")

// Estadisticas de demografia por tomar en cuenta:
// ("CODDIST", 0L) 3
// ("PROVINCIA", "") 4
// ("CANTON", "") 5
// ("DISTRITO", "") 6
// ("PobTotal", 0L) 7
// ("DensidadPob", 0L) 8
// ("PrcPobUrb", 0L) 9
// ("RelHomMuj", 0L) 10
// ("PrcPobMayor65", 0L) 12
// ("PrcPobNacExtranj", 0L) 13
// ("TasaFecund", 0L) 14
// ("PrcPobCasada", 0L) 15
// ("PrcPobDiscapacidad", 0L) 16

// Estadísticas en el dataset:
// 0: OBJECTID_12,
// 1: CODPROV,
// 2: CODCANT,
// 3: CODDIST,
// 4: PROVINCIA,
// 5: CANTON,
// 6: DISTRITO,
// 7: Poblacion_total,
// 8: Densidad_de_poblacion,
// 9: Porcentaje_de_población_urbana,
// 10: Relacion_hombres_mujeres,
// 11: Relacion_dependencia_demografica,
// 12: Porcentaje_de_poblacion_mayor_a_65,
// 13: Porcentaje_de_población_nacida_extranjero,
// 14: Tasa_fecundidad_general,
// 15: Porcentaje_de_personas_unidas_o_casadas,
// 16: Porcentaje_de_discapacidad,
// 17: Porcentaje_de_poblacion_no_asegurada

// El CODDIST va a ser la llave en estos nodos
var vertices = Array(
				(0L, (
                        ("PROVINCIA", ""),
                        ("CANTON", ""),
                        ("DISTRITO", ""),
                        ("PobTotal", 0D),
                        ("DensidadPob", 0D),
                        ("PrcPobUrbana", 0D),
                        ("RelHombreMujer", 0D),
                        ("PrcPobMayor65", 0D),
                        ("PrcPobNacExtranj", 0D),
                        ("TasaFecund", 0D),
                        ("PrcPobCasada", 0D),
                        ("PrcPobDiscapacidad", 0D)
                    )
                )
			)

//Array inicial de aristas
var edges = Array(
				Edge(0L,0L,0)
			)

// Estadisticas de demografia por tomar en cuenta:
// ("CODDIST", 0L) 3
// ("PROVINCIA", "") 4
// ("CANTON", "") 5
// ("DISTRITO", "") 6
// ("PobTotal", 0L) 7
// ("DensidadPob", 0L) 8
// ("PrcPobUrb", 0L) 9
// ("RelHomMuj", 0L) 10
// ("PrcPobMayor65", 0L) 12
// ("PrcPobNacExtranj", 0L) 13
// ("TasaFecund", 0L) 14
// ("PrcPobCasada", 0L) 15
// ("PrcPobDiscapacidad", 0L) 16

// Por cada línea leída que se lee se crea el nodo distrito respectivo
for (line <- bufferedSource.getLines) {
    var cols = line.split(",").map(_.trim)

    var id = s"${cols(3)}".toLong
    var provincia = s"${cols(4)}"
    var canton = s"${cols(5)}"
    var distrito = s"${cols(6)}"
    var pobTotal = s"${cols(7)}".toDouble
    var densidadPob = s"${cols(8)}".toDouble
    var prcPobUrb = s"${cols(9)}".toDouble
    var relHomMuj = s"${cols(10)}".toDouble
    var prcPobMayor65 = s"${cols(12)}".toDouble
    var prcPobNacExtranj = s"${cols(13)}".toDouble
    var tasaFecund = s"${cols(14)}".toDouble
    var prcPobCasada = s"${cols(15)}".toDouble
    var prcPobDiscapacidad = s"${cols(16)}".toDouble

    // Se crea la tupla
    var tupla = (id,    
                    (
                        ("PROVINCIA", provincia),
                        ("CANTON", canton),
                        ("DISTRITO", distrito),
                        ("POB_TOTAL", pobTotal),
                        ("DENS_POBLACION", densidadPob),
                        ("PRC_POB_URBANA", prcPobUrb),
                        ("REL_HOMBRE_MUJER", relHomMuj),
                        ("PRC_POB_MAYPR_65", prcPobMayor65),
                        ("PRC_POB_EXTRANJERA", prcPobNacExtranj),
                        ("TASA_FECUND", tasaFecund),
                        ("PRC_POB_CASADA", prcPobCasada),
                        ("PRC_POB_DISCAPACIDAD", prcPobDiscapacidad)
                    )
                    
                )
    
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


