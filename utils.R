ejecutar_comando_sistema <- function(comando){
  comando <- unlist(strsplit(comando, split = " ", fixed = T))
  system2(comando[1], args = comando[-1])
}

normalizar <- function(str){
  stringr::str_replace_all(str, "\\+", "") %>% 
    stringr::str_replace_all(., " ", "") %>% 
  stringi::stri_trans_general(., "latin-ascii")
}
