import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

//Lista de pasos:
//1. Crear los nodos vertices de distritos a partir de fuerza laboral
//CREAR ARRAY DE VERTICES COMPLETO
//2. Parsear los demas archivos de estadisticas y agregar las demas estadisticas, revisar si el distrito de los demas archivos existe en los distritos de fuerza laboral
//3. Parsear el de carreteras, revisar si el nsrc y ndest hace match con algun nodo en los distritos, si no hace match se descarta
//4. Si hace match, se crea la arista entre esos nodos que ya existen
//5. Con el map y group, ya se tiene el shape_length apropiado entonces se crea el grafo
//6. Hacer las consultas sobre el grafo

val archivoFuerzaLaboral = scala.io.Source.fromFile("Distritos_Fuerza_Laboral.csv")
val archivoHogares = scala.io.Source.fromFile("Distritos_Hogares.csv")
val archivoEducacion = scala.io.Source.fromFile("Distritos_Educacion.csv")
val archivoDemografico = scala.io.Source.fromFile("Distritos_Sociales_y_Demograficos.csv")
var archivoCarreteras = scala.io.Source.fromFile("Carreteras_de_Costa_Rica_noExtraComas_noEmptyRegs_SrcDstAdded.csv")


var verticesTotal = Array(
				(0L, (
                        ("PROVINCIA", ""),
                        ("CANTON", ""),
                        ("DISTRITO", ""),
                        ("PobFuerzaTrabajoTotal", 0D),
                        ("PobFuerzaTrabajoOcupada", 0D),
                        ("DesempTotal", 0D),
                        ("DesempConExperiencia", 0D),
                        ("DesempSinExperiencia", 0D),
                        ("PobFueraFuerzaTrabajoTotal", 0D),
                        ("PobFueraFuerzaTrabajoPensionadoJubilado", 0D),
                        ("PobFueraFuerzaTrabajoViveRenta", 0D),
                        ("PobFueraFuerzaTrabajoEstudiante", 0D),
                        ("PorcentajeOcupacion", 0D), // Atributo extra para guardar el porcentaje de ocupacion
                        //Estadisticas de Educacion
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
                        ("PrcPobDe7A17ConAlMenos1AnioDeRezago", 0D),
                        //Estadisticas de Demografico
                        ("PobTotal", 0D),
                        ("DensidadPob", 0D),
                        ("PrcPobUrbana", 0D),
                        ("RelHombreMujer", 0D),
                        ("PrcPobMayor65", 0D),
                        ("PrcPobNacExtranj", 0D),
                        ("TasaFecund", 0D),
                        ("PrcPobCasada", 0D),
                        ("PrcPobDiscapacidad", 0D),
                        //Estadisticas de Hogares
                        ("TotalViviendasIndividualesOcupadas", 0D),
                        ("PromedioOcupantesVivienda", 0D),
                        ("PorcentajePropias", 0D),
                        ("PorcentajeAlquiladas", 0D),
                        ("PorcentajeBuenEstado", 0D),
                        ("PorcentajeHacinadas", 0D)
                    )
                )
			)

var general = Array(
            (0L,(
                        ("PROVINCIA", ""),
                        ("CANTON", ""),
                        ("DISTRITO", "")
                    )
                )
            )

var arrayFuerza = Array(
				(0L, (
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
var arrayEducacion = Array(
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

var arrayDemografico = Array(
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

var arrayHogares = Array(
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


//*************** Lee archivo de Estadisticas de FUERZA LABORAL *******************//

for (line <- archivoFuerzaLaboral.getLines) {
    var cols = line.split(",").map(_.trim)

    var id = s"${cols(3)}".toLong
    var provincia = s"${cols(4)}"
    var canton = s"${cols(5)}"
    var distrito = s"${cols(6)}"
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
    // Se agrega al arreglo de nodos
    arrayFuerza = arrayFuerza :+ tupla
    verticesTotal = verticesTotal:+ arrayFuerza
}


//*************** Lee archivo de Estadisticas de EDUCACION *******************//

for (line <- archivoEducacion.getLines) {
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
                        ("PROVINCIA", ""),
                        ("CANTON", ""),
                        ("DISTRITO", ""),
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
    arrayEducacion = arrayEducacion :+ tupla
    //val findTuple = verticesTotal.filter(x => x._1 == id)
    //println(s"elemento en array educacion= ${arrayEducacion(${id})}")
    //println(s"elemento en array vertices total= ${verticesTotal(${id})}")
}

//Creacion de RDD
val fuerzaRDD : RDD[(Long, (String, String, String, Double,Double,Double,Double,Double,Double,
Double,Double,Double,Double))] = sc.parallelize(arrayFuerza)

val educacionRDD : RDD[(Long, (String, String, String, Double,Double,Double,Double,Double,Double,
Double,Double,Double,Double,Double,Double,Double))] = sc.parallelize(arrayEducacion)

var newVert = sc.parallelize(fuerzaRDD.join(educacionRDD).map{e => (e._1, (e._2._1._1,e._2._1._2,e._2._1._3,e._2._2._1,e._2._2._2,e._2._2._3,e._2._2._4))}.collect)






