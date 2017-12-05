import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

//**************************** RDD FUERZA LABORAL **************************** //

val archivoFuerzaLaboral = scala.io.Source.fromFile("Distritos_Fuerza_Laboral.csv")
var verticesDistrito = Array(
				(0L, (
                        ("PROVINCIA", ""),
                        ("CANTON", ""),
                        ("DISTRITO", ""),
                        ("PROVINCIA", ""),
                        ("CANTON", ""),
                        ("DISTRITO", "")
                        ("POB_MAYOR_15", 0D),
                        ("POB_FUERZA_TRABAJO_TOTAL", 0D),
                        ("POB_FUERZA_TRABAJO_OCUPADA", 0D),
                        ("DESEMP_TOTAL", 0D),
                        ("DESEMP_CON_EXPERIENCIA", 0D),
                        ("DESEMP_SIN_EXPERIENCIA", 0D),
                        ("POB_FUERA_FUERZA_TRABAJO_TOTAL", 0D),
                        ("POB_FUERA_FUERZA_TRABAJO_PENSIONADO_JUBILADO", 0D),
                        ("POB_FUERA_FUERZA_TRABAJO_VIVE_RENTA", 0D),
                        ("POB_FUERA_FUERZA_TRABAJO_ESTUDIANTE", 0D),
                        ("PORCENTAJE_OCUPACION", 0D) 
                    )
                )
			)

var edgesFuerzaLaboral = Array(
				Edge(0L,0L,"FuerzaLaboral")
			)

for (line <- archivoFuerzaLaboral.getLines) {
    var cols = line.split(",").map(_.trim)

    var id = s"${cols(3)}".toLong
    var idStat = s"${cols(1)}".toLong
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
    var tuplaDistrito = (id,
                    (
                        ("PROVINCIA", provincia),
                        ("CANTON", canton),
                        ("DISTRITO", distrito)
                        ("POB_MAYOR_15", 0D),
                        ("POB_MAYOR_15", 0D),
                        ("POB_FUERZA_TRABAJO_TOTAL", 0D),
                        ("POB_FUERZA_TRABAJO_OCUPADA", 0D),
                        ("DESEMP_TOTAL", 0D),
                        ("DESEMP_CON_EXPERIENCIA", 0D),
                        ("DESEMP_SIN_EXPERIENCIA", 0D),
                        ("POB_FUERA_FUERZA_TRABAJO_TOTAL", 0D),
                        ("POB_FUERA_FUERZA_TRABAJO_PENSIONADO_JUBILADO", 0D),
                        ("POB_FUERA_FUERZA_TRABAJO_VIVE_RENTA", 0D),
                        ("POB_FUERA_FUERZA_TRABAJO_ESTUDIANTE", 0D),
                        ("PORCENTAJE_OCUPACION", 0D) 
                    ) 
                )

var tuplaEstadistica = (idStat,
                    (
                        ("PROVINCIA", ""),
                        ("CANTON", ""),
                        ("DISTRITO", "")
                        ("POB_MAYOR_15", ""),
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
    
    // Se agrega al arreglo de nodos
    verticesDistrito = verticesDistrito :+ tuplaDistrito
    verticesDistrito = verticesDistrito :+ tuplaEstadistica
    var arista = Edge(id,idStat,"FuerzaLaboral")
    edgesFuerzaLaboral = edgesFuerzaLaboral :+ arista
}

var vRDDdistritos : RDD[(Long, (String, String))] = sc.parallelize(verticesDistrito)
var vRDDFuerzaLaboral : RDD[(Long, (String, Double))] = sc.parallelize(verticesFuerzaLaboral)
vRDDdistritos.persist()
var eRDDFuerzaLaboral = sc.parallelize(edgesFuerzaLaboral)
var graphFuerzaLaboral = Graph(vRDDFuerzaLaboral, eRDDFuerzaLaboral)
println("***Grafo Fuerza Laboral****")
graphFuerzaLaboral.collect.foreach(println)



//**************************** RDD Demograficos **************************** //

val archivoDemograficos = scala.io.Source.fromFile("Distritos_Sociales_y_Demograficos.csv")

var verticesDemograficos = Array(
				(0L, (
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
var edgesDemograficos = Array(
				Edge(0L,0L,"Demograficos")
			)

for (line <- archivoDemograficos.getLines) {
    var cols = line.split(",").map(_.trim)

    var id = s"${cols(3)}".toLong
    var idStat = s"${cols(1)}".toLong
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
    var verticesDemograficos = (idStat,    
                    (
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
    verticesDemograficos = verticesDemograficos :+ tupla
    var arista = Edge(id,idStat,"Demograficos")
    edgesDemograficos = edgesDemograficos :+ arista
}
var vRDDDemografico = sc.parallelize(verticesDemograficos)
var eRDDDemografico = sc.parallelize(edgesDemograficos)
var graphDemografico = Graph(vRDDDemografico, eRDDDemografico)
println("***Grafo Demografico****")
graphDemografico.collect.foreach(println)

//**************************** GRAFO Hogares **************************** //

val archivoHogares = scala.io.Source.fromFile("Distritos_Hogares.csv")
var verticesHogares = Array(
				(0L, (
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
var edgesHogares = Array(
				Edge(0L,0L,"Hogares")
			)

for (line <- archivoHogares.getLines) {
    var cols = line.split(",").map(_.trim)

    var id = s"${cols(3)}".toLong
    var idStat = s"${cols(1)}".toLong
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
                        ("TOTAL_VIVIENDAS_INDIV_OCUPADAS", totalViviendasIndivOcupadas),
                        ("PROM_OCUPANTES_VIVIENDA", promOcupantesVivienda),
                        ("PRC_PROPIAS", prcPropias),
                        ("PRC_ALQUILADAS", prcAlquiladas),
                        ("PRC_BUEN_ESTADO", prcBuenEstado),
                        ("PRC_HACINADAS", prcHacinadas)
                    )
                    
                )
    
    // Se agrega al arreglo de nodos
    verticesHogares = verticesHogares :+ tupla
    var arista = Edge(id,idStat,"Hogares")
    edgesHogares = edgesHogares :+ arista
}
var vRDDHogares = sc.parallelize(verticesHogares)
var eRDDHogares = sc.parallelize(edgesHogares)
var graphHogares = Graph(vRDDHogares, eRDDHogares)
println("***Grafo hogares****")
graphHogares.collect.foreach(println)

//**************************** GRAFO Educacion **************************** //
val archivoEducacion = scala.io.Source.fromFile("Distritos_Educacion.csv")

var verticesEducacion = Array(
				(0L, (
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
var edgesEducacion = Array(
				Edge(0L,0L,"Educacion")
			)

for (line <- bufferedSource.getLines) {
    var cols = line.split(",").map(_.trim)

    var id = s"${cols(3)}".toLong
    var idStat = s"${cols(1)}".toLong
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
    var tupla = (idStat,    
                    (
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
    
    // Se agrega al arreglo de nodos
    verticesEducacion = verticesEducacion :+ tupla
    var arista = Edge(id,idStat,"Educacion")
    edgesEducacion = edgesEducacion :+ arista
}

var vRDDEducacion = sc.parallelize(verticesEducacion)
var eRDDEducacion = sc.parallelize(edgesEducacion)
var graphEducacion = Graph(vRDDEducacion, eRDDEducacion)
println("***Grafo educacion****")
graphEducacion.collect.foreach(println)

//**************************** GRAFO CARRETERAS **************************** //

var archivoCarreteras = scala.io.Source.fromFile("Carreteras_de_Costa_Rica_noExtraComas_noEmptyRegs_SrcDstAdded.csv")
// Creación del array inicial de nodos

var verticesCarreteras = Array(
				(0L, (
                        ("NOM_DISTRITO", ""),
                        ("CABECERA_DE_CANTON", "")
                    )
                )
			)


//Array inicial de aristas (NSRC, NDST, DISTANCIA)
var edgesCarreteras = Array(
				Edge(0L,0L,0D)
			)

// Nodo default
var nowhere = "nowhere"
var idCounter = 0L
var idA = 0L
var idB = 0L

for (line <- archivoCarreteras.getLines) {

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
    var origen = (idCounter,    
                    (
                        ("NOM_DISTRITO", nsrc),
                        ("CABECERA_DE_CANTON", "")
                    )
                )

    if (

    idCounter = idCounter + 1
    idB = idCounter

    // Destino
    var destino = (idCounter,    
                    (
                        ("NOM_DISTRITO", ndst),
                        ("CABECERA_DE_CANTON", "")
                    )
                )


    // println(tupla)
    verticesCarreteras = verticesCarreteras :+ origen
    verticesCarreteras = verticesCarreteras :+ destino

    // Creación de arista

    var arista = Edge(idA, idB, shape_length) // Por ahora usa este shape_length a pesar de que no sea el exacto
    edgesCarreteras = edgesCarreteras :+ arista
}

var edgesTotal =Array(
				Edge(0L,0L,0D)
			)

var vRDDCarreteras= sc.parallelize(verticesCarreteras)
var eRDDCarreteras = sc.parallelize(edgesCarreteras)  

// Creación del grafo con RDD de nodos y aristas
println("--------------------------- CREACIÓN DE GRAFO ----------------------------")

var graphCarreteras = Graph(vRDDCarreteras, eRDDCarreteras)
