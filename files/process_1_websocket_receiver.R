# ##############################################################################
# # PROCESS 1: WebSocket Receiver                                              #
# ##############################################################################
# Purpose: connect to AIS stream, receive hex messages, buffer and save to disk                                                              
# Output: raw_hex_YYYYMMDD_nnn.txt files         
# Fast as fuck boi

# (0) Initialize  ----------------------------------------------------------

library(websocket)
library(jsonlite)

# (1) Configuration --------------------------------------------------------

# Source API key to https://aisstream.io/
ais_api_key <- source("files/ais_api_key.R")$value

# File paths
current_date <- format(Sys.Date(), "%Y-%m-%d")
data_dir <- "data/raw_hex"
if (!dir.exists(data_dir)) dir.create(data_dir, recursive = TRUE)

# Initialize file counter
file_counter <- 1
current_file <- paste0(data_dir, "/raw_hex_", current_date, "_", 
                       sprintf("%03d", file_counter), ".txt")

# Stats counter
total_messages <- 0
total_files_written <- 0

# Init buffer 
buffer_size <- 50000  # messages
buffer_time <- 15     # seconds
hex_buffer <- character(buffer_size)
buffer_index <- 0
last_flush <- Sys.time()

# (2) Helper Functions -----------------------------------------------------

# Flush buffer to disk
flush_buffer <- function() {
  if (buffer_index == 0) return()
  
  # Append accumulated hex messages to file
  con <- file(current_file, open = "w")
  writeLines(hex_buffer[1:buffer_index], con)
  close(con)
  
  # Update statistics
  total_files_written <<- total_files_written + 1
  total_messages <<- total_messages + buffer_index
  
  cat(sprintf("[FLUSH] Wrote %d messages to %s (Total: %d)\n", 
              buffer_index, basename(current_file), total_messages))
  
  
  
  # Reset buffer
  buffer_index <<- 0
  hex_buffer <<- character(buffer_size)
  last_flush <<- Sys.time()
  
  # Free memory
  gc(verbose = FALSE)
  
  # Iterate counter and file
  file_counter <<- file_counter + 1
  current_file <<- paste0(data_dir, "/raw_hex_", 
                          format(Sys.Date(), "%Y-%m-%d"), "_",
                          sprintf("%03d", file_counter), ".txt")
}

# ##############################################################################
# # (3) WebSocket Setup ------------------------------------------------------
# ##############################################################################

create_ais_socket <- function() {
  
  # Global subscription message (collect everything)
  sub_msg <- list(
    APIKey = ais_api_key,
    # Global bounding box - no need to restrict non-ports if fast enough
    BoundingBoxes = list(list(list(-90, -180), list(90, 180))),
    FilterMessageTypes = list("PositionReport", "ShipStaticData")
  )
  
  # Create websocket
  new_ws <<- WebSocket$new("wss://stream.aisstream.io/v0/stream", autoConnect = FALSE)
  
  # On connection open
  new_ws$onOpen(function(event) {
    # Must send request within 3s of opening connection
    cat("[SYSTEM] Socket opened.\n")
    new_ws$send(toJSON(sub_msg, auto_unbox = TRUE))
  })
  
  # On message received - buffer in memory
  new_ws$onMessage(function(event) {
    # Extract data 
    raw_data <-event$data
    # Hex bytes to hex string
    if (is.raw(raw_data)) {
      raw_data <- paste(format(raw_data), collapse = "")
    }
    
    # Add to buffer
    buffer_index <<- buffer_index + 1
    hex_buffer[buffer_index] <<- raw_data
    
    # Time since last flush
    time_elapsed <- as.numeric(difftime(Sys.time(), last_flush, units = "secs"))
    # If greater than 1000 messages/lines of hex or greater than 1 second
    if (buffer_index >= buffer_size || time_elapsed >= buffer_time) {
      # Write lines to file, clean memory, start a new file
      flush_buffer()
      # file_counter <<- file_counter + 1
      # current_file <<- paste0(data_dir, "/raw_hex_", current_date, "_", 
      #                         sprintf("%03d", file_counter), ".txt")
    }
  })
  
  # On close
  new_ws$onClose(function(event) {
    cat(sprintf("[WARN] Socket closed. Code: %d, Reason: %s\n", event$code))
  })
  
  # On error
  new_ws$onError(function(event) {
    cat(sprintf("[ERROR] Socket error: %s\n", event$message))
    # Log to error file
    write(sprintf("%s [WEBSOCKET ERROR] %s\n", Sys.time(), event$message),
          file = "logs/error_log.txt", append = TRUE)
  })
  # Connect and return
  new_ws$connect()
  return(new_ws)
}

# ##############################################################################
# # (4) Main Execution Loop --------------------------------------------------
# ##############################################################################

cat("------------------------------------------------------------------------------\n")
cat("PROCESS 1: WebSocket Receiver\n")
cat(sprintf("Output directory: %s\n", data_dir))
cat(sprintf("Buffer size: %d messages or %d second(s)\n", buffer_size, buffer_time))
cat("Kill switch: Create 'STOP_AIS.txt' file or press Ctrl+C\n")
cat("------------------------------------------------------------------------------\n\n")

# Reconnect settings
retry_delay <- 1 # 1s backoff to start
max_delay <- 60  # 60s cap for backoff
ws <- NULL       

# Main event loop
tryCatch({
  repeat {
    
    # Kill switch: check for stop file and write on close
    if (file.exists("STOP_AIS.txt")) {
      cat("\n[STOP] Emergency stop file detected. Shutting down...\n")
      flush_buffer()
      if (!is.null(ws)) ws$close()
      break
    }
    
    # If connection is 3 (closed) or NULL
    if (is.null(ws) || isTRUE(ws$readyState() == 3)) {
      
      # Remove old connection
      if(!is.null(ws)) ws <- NULL
      
      cat(sprintf("[RECONNECT] Attempting connection (Retry Delay: %.0fs)...\n", 
                  retry_delay))
      # Create/refresh socket
      ws <- create_ais_socket()
      
      # Wait up to 5 seconds for connection to establish
      wait_counter <- 0
      while (ws$readyState() == 0 && wait_counter < 50) {
        later::run_now()
        Sys.sleep(0.1)
        wait_counter <- wait_counter + 1
      }
      
      # Check connection and adjust retry delay
      if (ws$readyState() == 1) {
        cat("[SUCCESS] Connection established. Streaming started...\n\n")
        retry_delay <- 1 # Reset backoff on success
      } else {
        cat(sprintf("[FAIL] Connection failed. Retrying in %.0fs....\n", 
            retry_delay))
        Sys.sleep(retry_delay)
        retry_delay <- min(retry_delay * 2, max_delay) # Doubles each time
        next # Skip to next iteration
      }
    }
      # Process websocket events
      later::run_now()
      Sys.sleep(0.01)  
    # }
  }
}, interrupt = function(e) {
  cat("\n[INTERRUPT] Shutting down gracefully...\n")
  flush_buffer() 
  if (!is.null(ws)) ws$close()
}, error = function(e) {
  cat(sprintf("[ERROR] %s\n", e$message))
  write(sprintf("%s [FATAL] %s\n", Sys.time(), e$message),
        file = "logs/error_log.txt", append = TRUE)
  try(flush_buffer(), silent = FALSE)
  if (!is.null(ws)) try(ws$close(), silent = FALSE)
})

# ##############################################################################
# # (5) Cleanup and Summary --------------------------------------------------
# ##############################################################################

Sys.sleep(1)
cat("\n------------------------------------------------------------------------------\n")
cat(sprintf("PROCESS 1: Shutdown Complete %d messages, %d files\n", total_messages, total_files_written))
cat("------------------------------------------------------------------------------\n")