
//*************** Lee archivo de Estadisticas de DEMOGRAFICO *******************//


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
    arrayDemografico = arrayDemografico :+ tupla
}

//*************** Lee archivo de Estadisticas de HOGARES *******************//
for (line <- archivoHogares.getLines) {
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
    
    // Se agrega al arreglo de nodos
    arrayHogares = arrayHogares :+ tupla
}