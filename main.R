library(glue)
library(data.table)
library(dplyr)
library(futile.logger)
library(conexiones)
library(optparse)
library(lubridate)
source('utils.R')
source('funciones.R')
kPathHDFSDetalladas <- '/u01/dw/prod/stage/{pais}/ventas/{fecha}/*'
kPathHDFSDiarias <- '/u01/dw/prod/stage/br/snapshots/historico/pfmdiario_{mes}.parquet'
kComandoHadoop <- 'hadoop fs -get {path_hdfs} {path_destino_parquets}'

KPathFstDetalladasRoot <- 'datos/detalladas/raw/{pais}/'
KPathFstDetalladas <- paste0(KPathFstDetalladasRoot, '{fecha}/fst/')
KPathFstDiarias <- 'datos/diarias/raw/{pais}/{mes}/fst/'
KPathDestinoParquets <- 'datos/raw/{pais}/{fecha}/parquets'
KPathDestinoParquetsDiarias <- 'datos/diarias/raw/{pais}/{mes}/parquets'
KPathResultadoDetalladas <- '/u02/U01/parquets/detalladas/{pais}/{anio}/{mes}/{dia}/'
KPathResultadoDetalladasConsolidados <- '/u02/U01/parquets/detalladas/{pais}/{anio}/{mes}/consolidados/'
KPathResultadoDetalladasPdvs <- '/u02/U01/parquets/detalladas/pdvs/{pais}/{anio}/{mes}/{dia}/'
KPathResultadoDetalladasConsolidadosPdvs <- '/u02/U01/parquets/detalladas/pdvs/{pais}/{anio}/{mes}/consolidados/'


KPathResultadoDiarias <- '/u02/U01/parquets/detalladas/{pais}/{anio}/{mes}/{dia}/'
KPathResultadoDiariasConsolidados <- '/u02/U01/parquets/diarias/{pais}/{anio}/{mes}/consolidados/'

opt <- parametros_cl()
mes <- opt$mes
pais <- opt$pais
tipo <- opt$tipo

def_geo <- Dimensiones::ObtenerDefGeosDefault(pais)
def_canal <- Dimensiones::ObtenerDefCanalesDefault2(pais)
pdvs <- conexiones::dbGetQueryDT(glue('dw/{pais}'), glue('select pdv.codigo pdv_codigo, {def_geo} geo, {def_canal} canal from dw.d_punto_venta pdv join dwcla.d_punto_venta_modelo pdvm on pdvm.codigo_mm = pdv.codigo'))

extraer_detalladas <- function(pais, mes, pdvs){
  fecha_desde <- as.Date(paste0(mes, '01'), format('%Y%m%d'))
  # procesar_fsts(pdvs, fecha_desde)
  fecha_hasta <- lubridate::ceiling_date(as.Date(paste0(mes, '01'), format('%Y%m%d')), 'month') - 1
  fechas <- seq(fecha_desde, fecha_hasta, by = '1 day')
  

  lapply(fechas, function(fecha_date){
    fecha <- format(fecha_date, '%Y%m%d')
    anio <- as.integer(format(fecha_date, '%Y'))
    mes <- as.integer(format(fecha_date, '%m'))
    dia <- as.integer(format(fecha_date, '%d'))

    path_hdfs <- glue(kPathHDFSDetalladas)
    path_destino_parquets <- glue(KPathDestinoParquets)
    dir.create(path_destino_parquets, showWarnings = F, recursive = T)
    comando_hadoop <- glue(kComandoHadoop)

    flog.info('Extrayendo parquets para fecha %s', fecha)
    ejecutar_comando_sistema(comando_hadoop)
    flog.info('Fin: Extrayendo parquets para fecha %s', fecha)

    archivos_parquets <- list.files(glue(KPathDestinoParquets), pattern = 'parquet', full.names = T, recursive = T)
    archivos_parquets <- archivos_parquets[grepl('detalladas', archivos_parquets)]

    parquets_to_fst_geo_canal_detalladas(archivos_parquets, pdvs, fecha_date)

    gc()
    borrar_directorios_temporales(pais, fecha)

  })
  procesar_fsts_geo_canal_detalladas(pdvs, fecha_desde)
  procesar_fsts_pdvs_detalladas(pdvs, fecha_desde)
  flog.info('FIN')
}

extraer_diarias <- function(pais, mes, pdvs){
  
  fecha_date <- as.Date(paste0(mes, '01'), '%Y%m%d')
  
  path_hdfs <- glue(kPathHDFSDiarias)
  path_destino_parquets <- glue(KPathDestinoParquetsDiarias)
  dir.create(path_destino_parquets, showWarnings = F, recursive = T)
  path_destino_parquets <- glue(KPathDestinoParquetsDiarias)
  comando_hadoop <- glue(kComandoHadoop)
  
  ejecutar_comando_sistema(comando_hadoop)
  
  archivos_parquets <- list.files(path_destino_parquets, recursive = T, full.names = T, pattern = 'parquet')
  parquets_to_fst_geo_canal_diaria(archivos_parquets, pdvs, fecha_date, path_fst = KPathFstDiarias, columnas = KColumnasDiarias)
  procesar_fsts_diarias(pdvs, fecha_date)
  # borrar_directorios_temporales(pais, fecha_date)
}

if(tipo == 'detalladas'){
  extraer_detalladas(pais, mes, pdvs)
}else{
  extraer_diarias(pais, mes, pdvs)  
}



