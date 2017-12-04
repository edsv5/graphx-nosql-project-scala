// Este script lee el archivo csv del dataset de distritos de fuerza laboral, crea un nodo por cada distrito y asigna
// sus estadisticas. En este grafo, solo importan los nodos, no las aristas

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

val bufferedSource = scala.io.Source.fromFile("Distritos_Fuerza_Laboral.csv")

// Estadisticas de fuerza laboral por tomar en cuenta:
// ("CODDIST", 0L) 3
// ("PROVINCIA", "") 4
// ("CANTON", "") 5
// ("DISTRITO", "") 6
// ("Población_mayoy_15", 0L) 8
// ("Poblacion_Fuerza_Trabajo_Total", 0L) 9
// ("Poblacion_Fuerza_Trabajo_Ocupada", 0L) 10
// ("Desempleada_Total", 0L) 11
// ("Desempleada_Con_experiencia", 0L) 12
// ("Desempleada_Sin_experiencia", 0L) 13
// ("Población_fuera_de_fuerza_trabajo_Total", 0L) 14
// ("Población_fuera_de_fuerza_trabajo_Pensionado_jubilado", 0L) 15
// ("Población_fuera_de_fuerza_trabajo_Vive_de_renta", 0L) 16
// ("Población_fuera_de_fuerza_trabajo_Estudiante", 0L) 17

// Estadísticas en el dataset:
// 0: OBJECTID_12,
// 1: CODPROV,
// 2: CODCANT,
// 3: CODDIST,
// 4: PROVINCIA,
// 5: CANTON,
// 6: DISTRITO,
// 7: Provincia_cantón_distrito,
// 8: Población_mayoy_15,
// 9: Poblacion_Fuerza_Trabajo_Total ,
// 10: Poblacion_Fuerza_Trabajo_Ocupada,
// 11: Desempleada_Total ,
// 12: Desempleada_Con_experiencia,
// 13: Desempleada_Sin_experiencia,
// 14: Población_fuera_de_fuerza_trabajo_Total,
// 15: Población_fuera_de_fuerza_trabajo_Pensionado_jubilado,
// 17: Población_fuera_de_fuerza_trabajo_Vive_de_renta,
// 18: Población_fuera_de_fuerza_trabajo_Estudiante,
// 19: Población_fuera_de_fuerza_trabajo_Quehaceres_hogar,
// 20: Población_fuera_de_fuerza_trabajo_Otra_situación

// El CODDIST va a ser la llave en estos nodos
var vertices = Array(
				(0L, (
                        ("PROVINCIA", ""),
                        ("CANTON", ""),
                        ("DISTRITO", ""),
                        ("PobMayor15", 0D),
                        ("PobFuerzaTrabajoTotal", 0D),
                        ("PobFuerzaTrabajoOcupada", 0D),
                        ("DesempTotal", 0D),
                        ("DesempConExperiencia", 0D),
                        ("DesempSinExperiencia", 0D),
                        ("PobFueraFuerzaTrabajoTotal", 0D),
                        ("PobFueraFuerzaTrabajoPensionadoJubilado", 0D),
                        ("PobFueraFuerzaTrabajoViveRenta", 0D),
                        ("PobFueraFuerzaTrabajoEstudiante", 0D),
                        ("PorcentajeOcupacion", 0D) // Atributo extra para guardar el porcentaje de ocupacion
                    )
                )
			)

//Array inicial de aristas
var edges = Array(
				Edge(0L,0L,0)
			)

// Estadisticas de educacion por tomar en cuenta:
// ("CODDIST", 0L) 3
// ("PROVINCIA", "") 4
// ("CANTON", "") 5
// ("DISTRITO", "") 6
// ("PobMayor15", 0L) 8
// ("PobFuerzaTrabajoTotal", 0L) 9
// ("PobFuerzaTrabajoOcupada", 0L) 10
// ("DesempTotal", 0L) 11
// ("DesempConExperiencia", 0L) 12
// ("DesempSinExperiencia", 0L) 13
// ("PobFueraFuerzaTrabajoTotal", 0L) 14
// ("PobFueraFuerzaTrabajoPensionadoJubilado", 0L) 15
// ("PobFueraFuerzaTrabajoViveRenta", 0L) 16
// ("PobFueraFuerzaTrabajoEstudiante", 0L) 17

// Por cada línea leída que se lee se crea el nodo distrito respectivo
for (line <- bufferedSource.getLines) {
    var cols = line.split(",").map(_.trim)

    var id = s"${cols(3)}".toLong
    var provincia = s"${cols(4)}"
    var canton = s"${cols(5)}"
    var distrito = s"${cols(6)}"
    var pobMayor15 = s"${cols(8)}".toDouble
    var pobFuerzaTrabajoTotal = s"${cols(9)}".toDouble
    var pobFuerzaTrabajoOcupada = s"${cols(10)}".toDouble
    var desempTotal = s"${cols(11)}".toDouble
    var desempConExperiencia = s"${cols(12)}".toDouble
    var desempSinExperiencia = s"${cols(13)}".toDouble
    var pobFueraFuerzaTrabajoTotal = s"${cols(14)}".toDouble
    var pobFueraFuerzaTrabajoPensionadoJubilado = s"${cols(15)}".toDouble
    var pobFueraFuerzaTrabajoViveRenta = s"${cols(16)}".toDouble
    var pobFueraFuerzaTrabajoEstudiante = s"${cols(17)}".toDouble
    var porcentajeOcupacion = pobFuerzaTrabajoOcupada / pobFuerzaTrabajoTotal

    // Se crea la tupla
    var tupla = (id,    
                    (
                        ("PROVINCIA", provincia),
                        ("CANTON", canton),
                        ("DISTRITO", distrito),
                        ("POB_MAYOR_15", pobMayor15),
                        ("POB_FUERZA_TRABAJO_TOTAL", pobFuerzaTrabajoTotal),
                        ("POB_FUERZA_TRABAJO_OCUPADA", pobFuerzaTrabajoOcupada),
                        ("DESEMP_TOTAL", desempTotal),
                        ("DESEMP_CON_EXPERIENCIA", desempConExperiencia),
                        ("DESEMP_SIN_EXPERIENCIA", desempSinExperiencia),
                        ("POB_FUERA_FUERZA_TRABAJO_TOTAL", pobFueraFuerzaTrabajoTotal),
                        ("POB_FUERA_FUERZA_TRABAJO_PENSIONADO_JUBILADO", pobFueraFuerzaTrabajoPensionadoJubilado),
                        ("POB_FUERA_FUERZA_TRABAJO_VIVE_RENTA", pobFueraFuerzaTrabajoViveRenta),
                        ("POB_FUERA_FUERZA_TRABAJO_ESTUDIANTE", pobFueraFuerzaTrabajoEstudiante),
                        ("PORCENTAJE_OCUPACION", porcentajeOcupacion)
                    )
                    
                )
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

// Consulta 2: Ordenar e imprimir las rutas más largas (valor más largo de arista (ojo, esto nos sirve un montón))

//graph.inDegrees.join(airportVertices).sortBy(_._2._1, ascending=false).take(1)

val ranks = graph.pageRank(0.0001).vertices

val ranksNodos = ranks.sortBy(_._2._1, ascending=false).map(_._2._2)
ranksNodos.take(10)

// graph.vertices.sortBy(_.attr, ascending=false).map(case (_.attr, tupla) => id == 223)
//collect.foreach(println)

// graph.triplets.sortBy(_.attr, ascending=false).map(triplet =>
//    	"Distance " + triplet.attr.toString + // attr: Atributo de la arista, es decir, distancia
//		" from " + triplet.srcAttr + // srcAttr: atributo del nodo origen, en este caso, ID
//		" to " + triplet.dstAttr + "." // dstAttr: atributo del nodo destino, en este caso ID 
//	).collect. 
//foreach(println) // Esto para imprimir

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


