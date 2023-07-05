library(lubridate)

KColumnasDetalladas <- c(
  'fecha_comercial',
  'pdv_codigo',
  'prod_codigo',
  'prod_emp_codigo',
  'hora',
  'nro_ticket',
  'cant_vta',
  'imp_vta'
)

KColumnasDiarias <- c('fecha_comercial',
                      'prod_codigo',
                      'pdv_codigo',
                      'sum_cant_vta',
                      'sum_imp_vta')


parquets_to_fst_geo_canal_detalladas <- function(archivos_parquets, pdvs, fecha_date) {
  
  anio <- as.integer(format(fecha_date, '%Y'))
  mes <- as.integer(format(fecha_date, '%m'))
  fecha <- format(fecha_date, '%Y%m%d')
  dia <- as.integer(format(fecha_date, '%d'))
  
  directorio_fst <- glue(KPathFstDetalladas)
  dir.create(directorio_fst, recursive = T, showWarnings = F)
  i <- 0
  cantidad_archivos_procesados <- 1
  invisible(lapply(archivos_parquets, function(archivo) {
    flog.info('Transformando parquets: %s de %s ', cantidad_archivos_procesados, length(archivos_parquets))
    cantidad_archivos_procesados <<- cantidad_archivos_procesados + 1
    ventas_parquet <-
      arrow::read_parquet(
        archivo,
        col_select = c(
          'fecha_comercial',
          'pdv_codigo',
          'prod_codigo',
          'prod_emp_codigo',
          'hora',
          'nro_ticket',
          'cant_vta',
          'imp_vta'
        )
      ) %>%
      as.data.table()
    ventas <- ventas_parquet[pdvs, on = 'pdv_codigo', nomatch = 0]
    
    ventas_particionadas <- ventas %>%
      group_split(geo, canal)
    
    invisible(lapply(ventas_particionadas, function(ventas_geo_canal) {
      i <<- i + 1
      #ventas_geo_canal <- ventas_particionadas[[1]]
      geo <- ventas_geo_canal[[1, 'geo']]
      canal <- ventas_geo_canal[[1, 'canal']]
      
      geo_normalizado <- normalizar(geo)
      canal_normalizado <- normalizar(canal)
      
      archivo_guardar <-
        glue(
          file.path(glue(KPathFstDetalladas), 'temp_{geo_normalizado}_{canal_normalizado}_{i}.fst')
        )
      fst::write.fst(ventas_geo_canal, archivo_guardar)
      NULL
    }))
    NULL
  }))
}


parquets_to_fst_geo_canal_diaria <- function(archivos_parquets, pdvs, fecha_date, path_fst = KPathFstDiarias, columnas = KColumnasDiarias) {
  
  anio <- as.integer(format(fecha_date, '%Y'))
  mes <- as.integer(format(fecha_date, '%m'))
  fecha <- format(fecha_date, '%Y%m%d')
  dia <- as.integer(format(fecha_date, '%d'))
  
  directorio_fst <- glue(path_fst)
  dir.create(directorio_fst, recursive = T, showWarnings = F)
  i <- 0
  cantidad_archivos_procesados <- 1
  invisible(lapply(archivos_parquets, function(archivo) {
    flog.info('Transformando parquets: %s de %s ', cantidad_archivos_procesados, length(archivos_parquets))
    cantidad_archivos_procesados <<- cantidad_archivos_procesados + 1
    ventas_parquet <-
      arrow::read_parquet(
        archivo,
        col_select = columnas
      ) %>%
      as.data.table()
    ventas <- ventas_parquet[pdvs, on = 'pdv_codigo', nomatch = 0]
    
    ventas_particionadas <- ventas %>%
      group_split(geo, canal)
    
    invisible(lapply(ventas_particionadas, function(ventas_geo_canal) {
      i <<- i + 1
      #ventas_geo_canal <- ventas_particionadas[[1]]
      geo <- ventas_geo_canal[[1, 'geo']]
      canal <- ventas_geo_canal[[1, 'canal']]
      
      geo_normalizado <- normalizar(geo)
      canal_normalizado <- normalizar(canal)
      
      archivo_guardar <-
        glue(
          file.path(glue(path_fst), 'temp_{geo_normalizado}_{canal_normalizado}_{i}.fst')
        )
      fst::write.fst(ventas_geo_canal, archivo_guardar)
      NULL
    }))
    NULL
  }))
}


procesar_fsts_geo_canal_detalladas <- function(pdvs, fecha_date){
  
  fecha <- format(fecha_date, '%Y%m%d')
  anio <- as.integer(format(fecha_date, '%Y'))
  mes <- as.integer(format(fecha_date, '%m'))
  dia <- as.integer(format(fecha_date, '%d'))
  
  path_resultado <- glue(KPathResultadoDetalladasConsolidados)
  dir.create(path_resultado, showWarnings = F, recursive = T)
  geo_canal <- unique(pdvs[, .(geo, canal)])
  geo_canal_particionado <- geo_canal %>%
    group_split(geo, canal)
  
  invisible(lapply(geo_canal_particionado, function(p){
    #p <- geo_canal_particionado[[6]]
    geo <- normalizar(p[[1, 'geo']])
    canal <- normalizar(p[[1, 'canal']])
    flog.info('Consolidando %s %s', geo, canal)
    
    archivo_guardar_ventas <-
      glue(
        file.path(glue(KPathResultadoDetalladasConsolidados), 'consolidadas_{geo}_{canal}.fst')
      )
    
    archivo_guardar_dic <-
      glue(
        file.path(glue(KPathResultadoDetalladasConsolidados), 'dic_{geo}_{canal}.dic')
      )
    
     if(file.exists(archivo_guardar_ventas)){
       return(NULL)
     }
    
    
    archivos <- list.files(glue(KPathFstDetalladasRoot), pattern = paste0('temp_', geo,'_',canal), full.names = T, recursive = T)
    if(length(archivos) == 0){
      return()
    }
    k <- 1 
    consolidado <- lapply(archivos, function(a){
      flog.info('Consolidando %s %s :: leyendo %s de %s archivos', geo, canal, k, length(archivos))
      k <<- k + 1
      v <- fst::read_fst(a, as.data.table = T)
      v$geo <- NULL
      v$canal <- NULL
      v <- v[, id_ticket := stringi::stri_c(pdv_codigo, nro_ticket, sep = '-')]
      v$nro_ticket <- NULL
      v
    }) %>% rbindlist()
    
    
    flog.info('Consolidando %s %s :: generando diccionario', geo, canal)
    consolidado <- consolidado[order(prod_codigo)]
    consolidado <- consolidado[, row_num := .I]
    consolidado <- consolidado[, row_num_prod := fifelse(prod_codigo < 4, NA_integer_, row_num)]
    indices <- consolidado[, .(indice_inicio = min(row_num_prod, na.rm = T), indice_final = max(row_num_prod, na.rm = T))]
    
    consolidado$row_num <- NULL
    consolidado$row_num_prod <- NULL
    
    fst::write.fst(consolidado, archivo_guardar_ventas)
    fst::write.fst(indices, archivo_guardar_dic)
    
    NULL
  }))
}

procesar_fsts_pdvs_detalladas <- function(pdvs, fecha_date){
  
  fecha <- format(fecha_date, '%Y%m%d')
  anio <- as.integer(format(fecha_date, '%Y'))
  mes <- as.integer(format(fecha_date, '%m'))
  dia <- as.integer(format(fecha_date, '%d'))
  
  path_resultado <- glue(KPathResultadoDetalladasConsolidadosPdvs)
  dir.create(path_resultado, showWarnings = F, recursive = T)
  geo_canal <- unique(pdvs[, .(geo, canal)])
  geo_canal_particionado <- geo_canal %>%
    group_split(geo, canal)
  
  invisible(lapply(geo_canal_particionado, function(p){
    #p <- geo_canal_particionado[[6]]
    geo <- normalizar(p[[1, 'geo']])
    canal <- normalizar(p[[1, 'canal']])
    flog.info('Consolidando %s %s', geo, canal)
    
    archivo_guardar_ventas <-
      glue(
        file.path(glue(KPathResultadoDetalladasConsolidadosPdvs), 'consolidadas_{geo}_{canal}.fst')
      )
    
    archivo_guardar_dic <-
      glue(
        file.path(glue(KPathResultadoDetalladasConsolidadosPdvs), 'dic_{geo}_{canal}.dic')
      )
    
    if(file.exists(archivo_guardar_ventas) & file.exists(archivo_guardar_dic)){
      return(NULL)
    }
    
    
    archivos <- list.files(glue(KPathFstDetalladasRoot), pattern = paste0('temp_', geo,'_',canal), full.names = T, recursive = T)
    if(length(archivos) == 0){
      return()
    }
    k <- 1 
    consolidado <- lapply(archivos, function(a){
      flog.info('Consolidando %s %s :: leyendo %s de %s archivos', geo, canal, k, length(archivos))
      k <<- k + 1
      v <- fst::read_fst(a, as.data.table = T)
      v$geo <- NULL
      v$canal <- NULL
      v <- v[, id_ticket := stringi::stri_c(pdv_codigo, nro_ticket, sep = '-')]
      v$nro_ticket <- NULL
      #cada 100 limpio la basura
      if(k %% 500 == 0) gc()
      v
    }) %>% rbindlist()
    
    
    flog.info('Consolidando %s %s :: generando diccionario', geo, canal)
    # consolidado <- consolidado[order(pdv_codigo, prod_codigo)]
    setorder(consolidado, pdv_codigo, prod_codigo)  
    consolidado <- consolidado[, row_num := .I]
    consolidado <- consolidado[, row_num_prod := fifelse(prod_codigo < 4, NA_integer_, row_num)]
    indices <- consolidado[, .(indice_inicio_con_cb = min(row_num_prod, na.rm = T), indice_final_con_cb = max(row_num_prod, na.rm = T), indice_inicio_pdv = min(row_num), indice_final_pdv = max(row_num)), by = pdv_codigo]
    
    
    consolidado$row_num <- NULL
    consolidado$row_num_prod <- NULL
    
    fst::write.fst(consolidado, archivo_guardar_ventas)
    fst::write.fst(indices, archivo_guardar_dic)
    rm(consolidado)
    rm(indices)
    gc()
    NULL
  }))
}



procesar_fsts_diarias <- function(pdvs, fecha_date){
  
  anio <- as.integer(format(fecha_date, '%Y'))
  mes <- as.integer(format(fecha_date, '%m'))
  fecha <- format(fecha_date, '%Y%m%d')
  
  path_resultado <- glue(KPathResultadoDiariasConsolidados)
  dir.create(path_resultado, showWarnings = F, recursive = T)
  geo_canal <- unique(pdvs[, .(geo, canal)])
  geo_canal_particionado <- geo_canal %>%
    group_split(geo, canal)
  
  invisible(lapply(geo_canal_particionado, function(p){
    #p <- geo_canal_particionado[[6]]
    geo <- normalizar(p[[1, 'geo']])
    canal <- normalizar(p[[1, 'canal']])
    flog.info('Consolidando %s %s', geo, canal)
    archivos <- list.files(glue(KPathFstDiarias), pattern = paste0(geo,'_',canal), full.names = T)
    if(length(archivos) == 0){
      return()
    }
    consolidado <- lapply(archivos, function(a){
      v <- fst::read_fst(a)
      v$geo <- NULL
      v$canal <- NULL
      v
    }) %>% rbindlist()
    
    setnames(consolidado, c('sum_cant_vta', 'sum_imp_vta'),c('cant_vta', 'imp_vta'))
    
    fst::write.fst(consolidado, glue(file.path(KPathResultadoDiariasConsolidados, '{geo}_{canal}_True.fst')))
    
    NULL
  }))
}  




parametros_cl <- function(){
  option.list <- list(
    make_option(c("-m", "--mes"), type = "character", default = format(Sys.Date() - months(1), '%Y%m') ),
    make_option(c("-p", "--pais"), type = "character", default = 'br'),
    make_option(c("-t", "--tipo"), type = "character", default = 'detalladas')
  )
  opt <- parse_args(OptionParser(option_list = option.list))
  opt
}

borrar_directorios_temporales <- function(pais, fecha){
  unlink(glue('datos/raw/{pais}/{fecha}/'), recursive = TRUE)
}


