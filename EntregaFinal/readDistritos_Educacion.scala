// Este script lee el archivo csv del dataset de distritos y educacion, crea un nodo por cada distrito y asigna
// sus estadisticas de educacion. En este grafo, solo importan los nodos, no las aristas

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

val bufferedSource = scala.io.Source.fromFile("Distritos_Educacion.csv")

// Estadisticas de educacacion por tomar en cuenta:
// ("CODDIST", 0L) 3
// ("PROVINCIA", "") 4
// ("CANTON", "") 5
// ("DISTRITO", "") 6
// ("PrcPobMen5EnGuarderia", 0L) 7
// ("PrcPobMay65EnCentroDiurno", 0L) 8
// ("PrcPobAnalfabeta", 0L) 9
// ("PrcAsistenciaEducReg", 0L) 12
// ("PrcPobQueAsisteACentroPublicoEducReg", 0L) 13
// ("PrcPobQueAsisteACentroPrivadoEducReg", 0L) 14
// ("PrcPobde5a15EnEducGenBasica", 0L) 15
// ("PrcPobEnEducAbierta", 0L) 16
// ("PrcPobConAlMenos1AnioDeSec", 0L) 19
// ("PrcPobDe17oMasConEducSuperior", 0L) 22
// ("PrcPobDe17oMasConTituloUniv", 0L) 25
// ("EscolaridadPromedioDePobDe15yMas", 0L) 26
// ("PrcPobDe7A17ConAlMenos1AnioDeRezago", 0L) 29

// Estadísticas en el dataset:
// 0: OBJECTID_12,
// 1: CODPROV,
// 2: CODCANT,
// 3: CODDIST,
// 4: PROVINCIA,
// 5: CANTON,
// 6: DISTRITO,
// 7: Porcentaje_de_población_menor_,
// 8: Porcentaje_de_población_de_65_,
// 9: Porcentaje_de_población_analfa,
// 10: Porcentaje_de_hombres_analfabet,
// 11: Porcentaje_de_mujeres_analfabet,
// 12: Porcentaje_de_asistencia_a_la_e,
// 13: Porcentaje_de_población_que_as,
// 14: Porcentaje_de_población_que__1,
// 15: Porcentaje_de_población_de_5_a,
// 17: Porcentaje_de_población_que__2,
// 18: Porcentaje_de_hombres_que_asist,
// 19: Porcentaje_de_mujeres_que_asist,
// 20: Porcentaje_de_población_de_15_,
// 21: Porcentaje_de_hombres_de_15_añ,
// 22: Porcentaje_de_mujeres_de_15_añ,
// 23: Porcentaje_de_población_de_17_,
// 24: Porcentaje_de_hombres_de_17_añ,
// 25: Porcentaje_de_mujeres_de_17_añ,
// 26: Porcenta_Poblacion_TituloUniver,
// 27: Escolaridad_promedio_de_la_pobl,
// 28: Escolaridad_promedio_de_los_hom,
// 29: Escolaridad_promedio_de_las_muj,
// 30: Porcentaje_de_población_de_7_a,
// 31: Porcentaje_de_hombres_de_7_a_17,
// 32: Porcentaje_de_mujeres_de_7_a_17,
// 33: Porcentaje_de_población_de_5_1,
// 34: Porcenta_Computer,
// 35: Porcenta_Internet,
// 36: GlobalID

// El CODDIST va a ser la llave en estos nodos
var vertices = Array(
				(0L, (
                        ("PROVINCIA", ""),
                        ("CANTON", ""),
                        ("DISTRITO", ""),
                        ("PrcPobMen5EnGuarderia", 0D),
                        ("PrcPobMay65EnCentroDiurno", 0D),
                        ("PrcPobAnalfabeta", 0D),
                        ("PrcAsistenciaEducReg", 0D),
                        ("PrcPobQueAsisteACentroPublicoEducReg", 0D),
                        ("PrcPobQueAsisteACentroPrivadoEducReg", 0D),
                        ("PrcPobde5a15EnEducGenBasica", 0D),
                        ("PrcPobEnEducAbierta", 0D),
                        ("PrcPobConAlMenos1AnioDeSec", 0D),
                        ("PrcPobDe17oMasConEducSuperior", 0D),
                        ("PrcPobDe17oMasConTituloUniv", 0D),
                        ("EscolaridadPromedioDePobDe15yMas", 0D),
                        ("PrcPobDe7A17ConAlMenos1AnioDeRezago", 0D)
                    )
                )
			)

//Array inicial de aristas
var edges = Array(
				Edge(0L,0L,0)
			)

// Estadisticas de educacacion por tomar en cuenta:
// ("CODDIST", 0L) 3
// ("PROVINCIA", "") 4
// ("CANTON", "") 5
// ("DISTRITO", "") 6
// ("PrcPobMen5EnGuarderia", 0L) 7
// ("PrcPobMay65EnCentroDiurno", 0L) 8
// ("PrcPobAnalfabeta", 0L) 9
// ("PrcAsistenciaEducReg", 0L) 12
// ("PrcPobQueAsisteACentroPublicoEducReg", 0L) 13
// ("PrcPobQueAsisteACentroPrivadoEducReg", 0L) 14
// ("PrcPobde5a15EnEducGenBasica", 0L) 15
// ("PrcPobEnEducAbierta", 0L) 16
// ("PrcPobConAlMenos1AnioDeSec", 0L) 19
// ("PrcPobDe17oMasConEducSuperior", 0L) 22
// ("PrcPobDe17oMasConTituloUniv", 0L) 25
// ("EscolaridadPromedioDePobDe15yMas", 0L) 26
// ("PrcPobDe7A17ConAlMenos1AnioDeRezago", 0L) 29

// Por cada línea leída que se lee se crea el nodo distrito respectivo
for (line <- bufferedSource.getLines) {
    var cols = line.split(",").map(_.trim)

    var id = s"${cols(3)}".toLong
    var provincia = s"${cols(4)}"
    var canton = s"${cols(5)}"
    var distrito = s"${cols(6)}"
    var prcPobMen5EnGuarderia = s"${cols(7)}".toDouble
    var prcPobMay65EnCentroDiurno = s"${cols(8)}".toDouble
    var prcPobAnalfabeta = s"${cols(9)}".toDouble
    var prcAsistenciaEducReg = s"${cols(12)}".toDouble
    var prcPobQueAsisteACentroPublicoEducReg = s"${cols(13)}".toDouble
    var prcPobQueAsisteACentroPrivadoEducReg = s"${cols(14)}".toDouble
    var prcPobde5a15EnEducGenBasica = s"${cols(15)}".toDouble
    var prcPobEnEducAbierta = s"${cols(16)}".toDouble
    var prcPobConAlMenos1AnioDeSec = s"${cols(19)}".toDouble
    var prcPobDe17oMasConEducSuperior = s"${cols(22)}".toDouble
    var prcPobDe17oMasConTituloUniv = s"${cols(25)}".toDouble
    var escolaridadPromedioDePobDe15yMas = s"${cols(26)}".toDouble
    var prcPobDe7A17ConAlMenos1AnioDeRezago = s"${cols(29)}".toDouble

    // Se crea la tupla
    var tupla = (id,    
                    (
                        ("PROVINCIA", provincia),
                        ("CANTON", canton),
                        ("DISTRITO", distrito),
                        ("PRC_POB_MENOR_5_EN_GUARDERIA", prcPobMen5EnGuarderia),
                        ("PRC_MAYOR_65_EN_CENTRO_DIURNO", prcPobMay65EnCentroDiurno),
                        ("PRC_POB_ANALFABETA", prcPobAnalfabeta),
                        ("PRC_ASISTENCIA_A_EDUCACION_REGULAR", prcAsistenciaEducReg),
                        ("PRC_POB_QUE_ASISTE_A_CENTRO_PUBLICO_EDUCACION", prcPobQueAsisteACentroPublicoEducReg),
                        ("PRC_POB_QUE_ASISTE_A_CENTRO_PRIVADO_EDUCACION", prcPobQueAsisteACentroPrivadoEducReg),
                        ("PRC_POB_DE_5_A_15_QUE_ASISTE_A_ED_GEN_BASICA", prcPobde5a15EnEducGenBasica),
                        ("PRC_POB_EN_EDUCACION_ABIERTA", prcPobEnEducAbierta),
                        ("PRC_POB_CON_AL_MENOS_UN_ANIO_DE_SECUNDARIA", prcPobConAlMenos1AnioDeSec),
                        ("PRC_POB_DE_17_O_MAS_CON_EDUC_SUPERIOR", prcPobDe17oMasConEducSuperior),
                        ("PRC_POB_DE_17_O_MAS_CON_TITULO_UNIV", prcPobDe17oMasConTituloUniv),
                        ("ESCOLARIDAD_PROMEDIO_15_ANIOS_O_MAS", escolaridadPromedioDePobDe15yMas),
                        ("PRC_POB_DE_7_A_17_CON_AL_MENOS_1_ANIO_REZAGO", prcPobDe7A17ConAlMenos1AnioDeRezago)
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
//println("Cantidad de distritos (cantidad de nodos)")
//val numDistritos = graph.numVertices

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


