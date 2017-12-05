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
			(0L,
			(
		                ("PROVINCIA", ""),
		                ("CANTON", ""),
		                ("DISTRITO", ""),
                        //Estadisticas de Fuerza laboral
		                ("PobFuerzaTrabajoTotal", 0D),
		                ("DesempTotal", 0D),
		                ("DesempConExperiencia", 0D),
		                ("PobFueraFuerzaTrabajoEstudiante", 0D),
		                ("PorcentajeOcupacion", 0D),
                        //Estadisticas de Educacion
		                ("PrcPobAnalfabeta", 0D),
		                ("PrcAsistenciaEducReg", 0D),
		                ("PrcPobConAlMenos1AnioDeSec", 0D),
		                ("PrcPobDe17oMasConTituloUniv", 0D),
		                //Estadisticas de Demografico
		                ("PobTotal", 0D),
		                ("RelHombreMujer", 0D),
		                ("PrcPobMayor65", 0D),
		                ("PrcPobNacExtranj", 0D),
		                ("PrcPobDiscapacidad", 0D),
		                //Estadisticas de Hogares
		                ("TotalViviendasIndividualesOcupadas", 0D),
		                ("PromedioOcupantesVivienda", 0D),
		                ("PorcentajePropias", 0D),
		                ("PorcentajeAlquiladas", 0D),
		                ("PorcentajeBuenEstado", 0D)
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

var distritos = Array((0L,""))

var arrayFuerza = Array(
				(0L, (
			("PROVINCIA", ""),
			("CANTON", ""),
			("DISTRITO", ""),
		                ("PobFuerzaTrabajoTotal", 0D),
		                ("DesempTotal", 0D),
		                ("DesempConExperiencia", 0D),
		                ("PobFueraFuerzaTrabajoEstudiante", 0D),
		                ("PorcentajeOcupacion", 0D) // Atributo extra para guardar el porcentaje de ocupacion
                        )
                )
            )
var arrayEducacion = Array(
				(0L, (
		                ("PrcPobAnalfabeta", 0D),
		                ("PrcAsistenciaEducReg", 0D),
		                ("PrcPobConAlMenos1AnioDeSec", 0D),
		                ("PrcPobDe17oMasConTituloUniv", 0D)
                    )
                )
            )

var arrayDemografico = Array(
				(0L, (
		                ("PobTotal", 0D),
		                ("RelHombreMujer", 0D),
		                ("PrcPobMayor65", 0D),
		                ("PrcPobNacExtranj", 0D),
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
		                ("PorcentajeBuenEstado", 0D)
                    )
                )
            )

var edges = Array(
				Edge(0L,0L,0D)
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
                        ("DESEMP_TOTAL", desempTotal),
                        ("DESEMP_CON_EXPERIENCIA", desempConExperiencia),
                        ("POB_FUERA_FUERZA_TRABAJO_ESTUDIANTE", pobFueraFuerzaTrabajoEstudiante),
                        ("PORCENTAJE_OCUPACION", porcentajeOcupacion)
        ))
    var tpl = (id,distrito)
    // Se agrega al arreglo de nodos
    arrayFuerza = arrayFuerza :+ tupla
    distritos = distritos :+ tpl
    
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
                        ("PRC_POB_ANALFABETA", prcPobAnalfabeta),
                        ("PRC_ASISTENCIA_A_EDUCACION_REGULAR", prcAsistenciaEducReg),
                        ("PRC_POB_CON_AL_MENOS_UN_ANIO_DE_SECUNDARIA", prcPobConAlMenos1AnioDeSec),
                        ("PRC_POB_DE_17_O_MAS_CON_TITULO_UNIV", prcPobDe17oMasConTituloUniv)
                    )
                )

    // Se agrega al arreglo de nodos
    arrayEducacion = arrayEducacion :+ tupla
}

//*************** Lee archivo de Estadisticas de HOGARES *******************//

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
                        ("PRC_BUEN_ESTADO", prcBuenEstado)
                    ) 
                )
    
    // Se agrega al arreglo de nodos
    arrayHogares = arrayHogares :+ tupla
}

//*************** Lee archivo de Estadisticas de DEMOGRAFICOS *******************//

for (line <- archivoDemografico.getLines) {
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
                        ("POB_TOTAL", pobTotal),
                        ("REL_HOMBRE_MUJER", relHomMuj),
                        ("PRC_POB_MAYPR_65", prcPobMayor65),
                        ("PRC_POB_EXTRANJERA", prcPobNacExtranj),
                        ("PRC_POB_DISCAPACIDAD", prcPobDiscapacidad)
                    )
                    
                )
    
    // Se agrega al arreglo de nodos
    arrayDemografico = arrayDemografico :+ tupla
}

//Creacion de RDD
val fuerzaRDD : RDD[(Long, ((String,String),(String,String),(String,String),(String,Double), (String,Double), (String,Double),(String,Double),(String,Double)))] = sc.parallelize(arrayFuerza)

val educacionRDD : RDD[(Long, ((String,Double),(String,Double),(String,Double),(String,Double)))] = sc.parallelize(arrayEducacion)

//rddFuerzaEdu.foreach(println)

var rddFuerzaEdu = sc.parallelize(fuerzaRDD.join(educacionRDD).map{distrito => (
		distrito._1,
		(
		 distrito._2._1._1,
		 distrito._2._1._2,
		 distrito._2._1._3,
         distrito._2._1._4,
         distrito._2._1._5,
         distrito._2._1._6, 
         distrito._2._1._7, 
         distrito._2._1._8, 
		 distrito._2._2._1,
		 distrito._2._2._2,
		 distrito._2._2._3,
		 distrito._2._2._4)
		)}.collect)

//rddFuerzaEdu.foreach(println)

val hogaresRDD : RDD[(Long, ((String,Double),(String,Double),(String,Double),(String,Double),(String,Double)))] = sc.parallelize(arrayHogares)

var rddFuerzaEduHogares = sc.parallelize(rddFuerzaEdu.join(hogaresRDD).map{distrito => (
		distrito._1,
		(
		 distrito._2._1._1,
		 distrito._2._1._2,
		 distrito._2._1._3,
         distrito._2._1._4,
         distrito._2._1._5,
         distrito._2._1._6, 
         distrito._2._1._7, 
         distrito._2._1._8, 
         distrito._2._1._9,
         distrito._2._1._10,
         distrito._2._1._11, 
         distrito._2._1._12,     
		 distrito._2._2._1,
		 distrito._2._2._2,
		 distrito._2._2._3,
		 distrito._2._2._4,
         distrito._2._2._5)
		)}.collect)

//rddFuerzaEduHogares.foreach(println)

val demograficosRDD : RDD[(Long, ((String,Double),(String,Double),(String,Double),(String,Double),(String,Double)))] = sc.parallelize(arrayDemografico)

var rddFuerzaEduHogaresDemo = sc.parallelize(rddFuerzaEduHogares.join(demograficosRDD).map{distrito => (
		distrito._1,
		(
		 distrito._2._1._1,
		 distrito._2._1._2,
		 distrito._2._1._3,
         distrito._2._1._4,
         distrito._2._1._5,
         distrito._2._1._6, 
         distrito._2._1._7, 
         distrito._2._1._8, 
         distrito._2._1._9,
         distrito._2._1._10,
         distrito._2._1._11, 
         distrito._2._1._12,
         distrito._2._1._13, 
         distrito._2._1._14,
         distrito._2._1._15,
         distrito._2._1._16,
         distrito._2._1._17,   
		 distrito._2._2._1,
		 distrito._2._2._2,
		 distrito._2._2._3,
		 distrito._2._2._4,
         distrito._2._2._5)
		)}.collect)

rddFuerzaEduHogaresDemo.foreach(println)

for (line <- archivoCarreteras.getLines) {

    var cols = line.split(",").map(_.trim)

    var nsrc = s"${cols(4)}" 
    var ndst = s"${cols(5)}"
    var shape_length = s"${cols(6)}".toDouble
    var dist1 : Long = 0
    var dist2 : Long = 0

    var dataFiltered = distritos.find(x => x._2 == nsrc) match {
        case Some((x,y)) => dist1 = x
	case None => dist1 = 0
    }

    var dataFiltered2 = distritos.find(x => x._2 == ndst) match {
        case Some((x,y)) => dist2 = x
	case None => dist2 = 0
    }

    // arista
    if((dist1 != 0) || (dist2 != 0)){
	var arista = Edge(dist1.toLong,dist2.toLong,shape_length)
	edges = edges :+ arista
    }
}

var eRDD = sc.parallelize(edges)

println("--------------------------- CREACIÓN DE GRAFO ----------------------------")
var graph = Graph(rddFuerzaEduHogaresDemo, eRDD)

println("-------------------------- IMPRESIÓN DE NODOS ----------------------------")
graph.vertices.saveAsTextFile("nodosCarreteras")
graph.vertices.collect.foreach(println)

println("-------------------------- IMPRESIÓN DE ARISTAS ----------------------------")
graph.edges.saveAsTextFile("aristasCarreteras")
graph.edges.collect.foreach(println)

archivoFuerzaLaboral.close
archivoHogares.close
archivoEducacion.close
archivoDemografico.close
archivoCarreteras.close
