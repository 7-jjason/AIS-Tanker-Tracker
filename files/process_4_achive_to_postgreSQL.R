library(data.table)
library(RPostgres)
library(DBI)
library(tidyverse)

setwd("/Users/josephjason/Documents/Forecasting/R/projects/AIS Tanker Tracker")

# initialize
wait_time <- 60 * 60 * 12
get_path <- "data/archived"
db <- 'Petroleum'
host_db <- 'localhost'
db_port <- '5432'
db_user <- 'josephjason'
db_password <- source('files/db_password.R')$value
drv <- Postgres()
dynamic_table_name <- 'archived_ais_dynamic'
static_table_name <- 'archived_ais_static'
temp_table_name <- 'temporary_table'

# ############################################################################
# # Main Execution Loop #
# ############################################################################

tryCatch({
  repeat{
    # kill switch: check for stop file and write on close
    if (file.exists("STOP_AIS.txt")) {
      cat("\n[STOP] Emergency stop file detected. Shutting down...\n")
      break
    }
    
    # get filenames
    all_files <- list.files(get_path, full.names = TRUE)
    dynamic_filenames <- grep("dynamic", all_files, value = TRUE)
    static_filenames <- grep("static", all_files, value = TRUE)
    
    # get files 
    dynamic_files <- lapply(dynamic_filenames, readRDS)
    static_files <- lapply(static_filenames, readRDS)
    
    # bind lists
    dynamic_files <- rbindlist(dynamic_files, fill = TRUE)
    static_files <- rbindlist(static_files, fill = TRUE)
    
    # remove duplicates
    dynamic_files <- dynamic_files[!duplicated(dynamic_files), ]
    static_files <- static_files[!duplicated(static_files), ]
    
    # create connection to database
    con <- dbConnect(drv, 
                     dbname = db, 
                     host = host_db, 
                     port = db_port,
                     user = db_user, 
                     password = db_password)
    
    # store dynamic data temporarily
    dbWriteTable(con, 
                 temp_table_name, 
                 dynamic_files,
                 temporary = TRUE,
                 overwrite = TRUE,
                 row.names = FALSE)
    
    # insert into main dynamic table
    dbExecute(con, "
          insert into archived_ais_dynamic
          select * from temporary_table
          on conflict do nothing
          ")
    
    # store static_data_temporarily
    dbWriteTable(con, 
                 temp_table_name, 
                 static_files,
                 temporary = TRUE,
                 overwrite = TRUE,
                 row.names = FALSE)
    
    # insert into main static table
    dbExecute(con, "
          insert into archived_ais_static
          select * from temporary_table
          on conflict do nothing
          ")
    
    # get data
    # data <- dbGetQuery(con, paste0("select * from ", table_name))
    
    # close connection
    dbDisconnect(con)
    
    gc()
    rm(all_files, dynamic_filenames, static_filenames, dynamic_files, static_files)
    
    Sys.sleep(wait_time)
  }
}, error = function(e) {
  cat("[ERROR] Process 4 - Archive to PostgreSQL")
})

