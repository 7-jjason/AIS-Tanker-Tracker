# ##############################################################################
# # Process 2: Parser & Filter                                                 #
# ##############################################################################
# Purpose: Extract key fields from hex before parsing, filter for tankers
# Input: raw_hex_YYYYMMDD_*.txt files
# Output: filtered_static_YYYYMMDD_nnn.rds, filtered_dynamic_YYYYMMDD_nnn.rds

# ##############################################################################
# # (0) Initialize  ----------------------------------------------------------
# ##############################################################################

library(data.table)
library(jsonlite)
library(parallel)
# ##############################################################################
# # (1) Configuration --------------------------------------------------------
# ##############################################################################

# Directories
raw_hex_dir <- "data/raw_hex"
filtered_dir <- "data/filtered"
if (!dir.exists(filtered_dir)) dir.create(filtered_dir, recursive = TRUE)
if (!dir.exists("logs")) dir.create("logs")

# Processing parameters
chunk_size <- 100000  # Process 25k hex lines at a time

# Hex patter definitions (fixed positions - never change in ais protocol)

# Message type markers
HEX_STATIC <- "225368697053746174696344617461"      # "ShipStaticData"
HEX_POSITION <- "22506f736974696f6e5265706f7274"    # "PositionReport"

# Field markers (field name in hex, followed by colon and value)
HEX_TYPE <- "2254797065223a"                        # "Type":
HEX_MMSI <- "224d4d5349223a"                        # "MMSI":
HEX_LATITUDE <- "226c61746974756465223a"            # "latitude":
HEX_LONGITUDE <- "226c6f6e676974756465223a"         # "longitude":
HEX_TIME_UTC <- "2274696d655f757463223a22"          # "time_utc":"
HEX_SHIP_NAME <- "22536869704e616d65223a22"         # "ShipName":"

# Tanker type range in hex (Types 80-89, where the second digits hex is 30-39 (the hex for ascii 0-9))
HEX_TANKER_PATTERN_START <- "2254797065223a38"     # "Type":8

# Tanker MMSI registry (built during processing)
registry_file <- "data/tanker_mmsi_registry.rds"
if (file.exists(registry_file)) {
  tanker_mmsis <- readRDS(registry_file)
} else {
  tanker_mmsis <- integer()
}

# Load existing registry - comment out after first run
# registry_csv <- "data/ais_vessel_spec_data.csv"
# if (file.exists(registry_csv)) {
#   tanker_mmsis <- fread(registry_csv)[[2]]
#   cat(sprintf("[REGISTRY] Loaded %d tanker MMSIs\n", length(tanker_mmsis)))
# } else {
#   cat("[REGISTRY] Starting with empty registry\n")
# }

# File counters
static_file_counter <- 1
dynamic_file_counter <- 1

# Statistics
stats_counter <- list(
  total_hex_messages = 0,
  hex_filtered_static = 0,
  hex_filtered_position = 0,
  hex_filtered_tankers = 0,
  parsed_static_tankers = 0,
  parsed_dynamic = 0,
  dynamic_kept = 0,
  dynamic_filtered_out = 0,
  dynamic_deduped = 0,
  parse_errors = 0
)

# ##############################################################################
# # (2) Hex Extraction Functions ---------------------------------------------
# ##############################################################################

# Extract numeric value after a hex field marker
# Returns: character string of the extracted value
extract_hex_value <- function(hex_string, marker, max_chars = 20) {
  # Find marker position
  marker_pos <- regexpr(marker, hex_string, fixed = TRUE)
  if (marker_pos == -1) return(NA_character_)
  
  # Start reading after marker
  start_pos <- marker_pos + nchar(marker)
  
  # Extract hex pairs until we hit a delimiter (comma=2c, quote=22, brace=7d)
  # Read up to max_chars hex pairs (* 2 characters - end position)
  value_hex <- substr(hex_string, start_pos, start_pos + max_chars * 2 - 1)
  
  # Find first delimiter
  delimiters <- c("2c", "22", "7d", "2e")  # comma, quote, brace, period
  first_delim_pos <- nchar(value_hex) + 1
  
  for (delim in delimiters) {
    pos <- regexpr(delim, value_hex, fixed = TRUE)
    if (pos != -1 && pos < first_delim_pos) {
      first_delim_pos <- pos
    }
  }
  
  # Extract value hex (before delimiter)
  value_hex <- substr(value_hex, 1, first_delim_pos - 1)
  
  # Convert hex to ASCII
  if (nchar(value_hex) == 0) return(NA_character_)
  
  tryCatch({
    # Split hex into 2 character pairs
    # Create pairs using start and end positions 
    hex_pairs <- strsplit(gsub("(.{2})", "\\1 ", value_hex), " ")[[1]]
    raw_bytes <- as.raw(strtoi(hex_pairs, 16L))
    rawToChar(raw_bytes)
  }, error = function(e) NA_character_)
}

# Check this is a tanker (Type 80-89) TRUE/FALSE
is_tanker_hex <- function(hex_string) {
  # Get position just before the second digit in "Type":8_
  type_pos <- regexpr(HEX_TANKER_PATTERN_START, hex_string, fixed = TRUE)
  if (type_pos == -1) return(FALSE)
  
  # Get position for second digit
  second_digit_pos <- type_pos + nchar(HEX_TANKER_PATTERN_START)
  # Check the second digit
  second_digit_hex <- substr(hex_string, second_digit_pos, second_digit_pos + 1)
  
  # Valid: 30-39 (ascii 0-9 for second digit in Type: 8_)
  return(second_digit_hex %in% c("30", "31", "32", "33", "34", 
                                 "35", "36", "37", "38", "39"))
}

# Extract MMSI from hex
extract_mmsi_hex <- function(hex_string) {
  mmsi_str <- extract_hex_value(hex_string, HEX_MMSI, max_chars = 10)
  if (is.na(mmsi_str)) return(NA_integer_)
  as.integer(mmsi_str)
}

# Extract timestamp minute for deduplication 
extract_time_minute_hex <- function(hex_string) {
  # Extract time_utc value
  time_str <- extract_hex_value(hex_string, HEX_TIME_UTC, max_chars = 30)
  if (is.na(time_str)) return(NA_character_)
  
  # Parse to minute (format: "2026-01-25 16:36:10.xxx")
  # Extract first 16 characters: "YYYY-MM-DD hh:mm"
  if (nchar(time_str) >= 16) {
    return(substr(time_str, 1, 16))
  }
  return(NA_character_)
}

# ##############################################################################
# # (3) Full JSON Parsing (only for filtered messages) ----------------------
# ##############################################################################

# Parse hex to JSON (only called AFTER hex filtering)
hex_to_json_full <- function(hex_string) {
  tryCatch({
    hex_pairs <- strsplit(gsub("(.{2})", "\\1 ", hex_string), " ")[[1]]
    raw_bytes <- as.raw(strtoi(hex_pairs, 16L))
    json_text <- rawToChar(raw_bytes)
    fromJSON(json_text)
  }, error = function(e) NULL)
}

# Parse static messages (already only tankers)
parse_static_tanker <- function(hex_string) {
  msg <- hex_to_json_full(hex_string)
  if (is.null(msg)) return(NULL)
  
  static_data <- msg$Message$ShipStaticData
  meta <- msg$MetaData
  
  # Double-check it's non-empty and a tanker 
  if (is.null(static_data$Type)) return(NULL)
  if (static_data$Type < 80 || static_data$Type > 89) return(NULL)
  
  # Build data.table row
  data.table(
    # Maritime Mobile Service Identity number - changes with owernship or flag
    mmsi = meta$MMSI,
    # Coordinated universal time
    time_utc = as.POSIXct(meta$time_utc, 
                          tz = "UTC"),
    ship_name = trimws(meta$ShipName),
    # International Maritime Organization number - never changes
    imo = if (is.null(static_data$ImoNumber)) NA_integer_ else static_data$ImoNumber,
    ship_type = static_data$Type,
    # Draught / draft - elevation of vessels hull relative to waterline
    draught = if (is.null(static_data$MaximumStaticDraught)) NA_real_ else static_data$MaximumStaticDraught,
    dest = if (is.null(static_data$Destination)) NA_character_ else trimws(static_data$Destination),
    # Dimension A - antenna to bow; B - to stern; C - to port; D - to startboard
    length_m = if (is.null(static_data$Dimension$A) || is.null(static_data$Dimension$B)) NA_real_ 
    else static_data$Dimension$A + static_data$Dimension$B,
    breadth_m = if (is.null(static_data$Dimension$C) || is.null(static_data$Dimension$D)) NA_real_ 
    else static_data$Dimension$C + static_data$Dimension$D,
    lat = meta$latitude,
    lon = meta$longitude
  )
}

# Parse dynamic message (only tanker MMSIs reach this point)
parse_dynamic <- function(hex_string) {
  msg <- hex_to_json_full(hex_string)
  if (is.null(msg)) return(NULL)
  
  pos <- msg$Message$PositionReport
  meta <- msg$MetaData
  
  data.table(
    mmsi = meta$MMSI,
    time_utc = as.POSIXct(meta$time_utc, 
                          # format = "%Y-%m-%d %H:%M:%0S",
                          tz = "UTC"),
    ship_name = trimws(meta$ShipName),
    lat = meta$latitude,
    lon = meta$longitude,
    # Speed over ground (knots)
    sog = if (is.null(pos$Sog)) NA_real_ else pos$Sog,
    # Course over ground (degrees)
    cog = if (is.null(pos$Cog)) NA_real_ else pos$Cog,
    # (degrees per minute)
    rate_of_turn = if (is.null(pos$RateOfTurn)) NA_real_ else pos$RateOfTurn,
    # (sailing, anchored, berthing, etc.)
    nav_stat = if (is.null(pos$NavigationalStatus)) NA_integer_ else pos$NavigationalStatus
  )
}

# ##############################################################################
# # (4) Batch Processing with Hex Pre-filtering -----------------------------
# ##############################################################################

process_hex_batch <- function(hex_lines) {
  
  # #########################
  # # (a) Hex Pre-Filtering #
  # #########################
  
  cat(sprintf("Filtering %d messages by hex patterns...\n", length(hex_lines)))
  
  # Filter by message type using hex patterns - get T/F vector, then filter
  is_static <- grepl(HEX_STATIC, hex_lines, fixed = TRUE)
  is_position <- grepl(HEX_POSITION, hex_lines, fixed = TRUE)
  
  # Filter static messages for tankers (Type 80-89) using hex
  static_hex <- hex_lines[is_static]
  
  # Is tanker check - get vector
  is_tanker_vec <- sapply(static_hex, is_tanker_hex)
  # Filter by tanker check vector
  tanker_hex_lines <- static_hex[is_tanker_vec]
  
  cat(sprintf("Found: %d static (%d tankers), %d position\n",
              sum(is_static), length(tanker_hex_lines), sum(is_position)))
  
  # ####################################
  # # (b) Parse Static Tanker Messages # 
  # ####################################
  
  cat(sprintf("Parsing %d tanker static messages...\n", length(tanker_hex_lines)))
  
  # Parse tanker static messages in parallel (6 cores)
  static_list <- mclapply(tanker_hex_lines, parse_static_tanker, mc.cores = 6)
  static_parse_errors <- sum(sapply(static_list, is.null))  # Count NULLs
  static_list <- rbindlist(static_list[!sapply(static_list, is.null)])
  
  # Update tanker MMSI registry - should i have this as a hashmap / env 
  if (nrow(static_list) > 0) {
    new_mmsis <- unique(static_list$mmsi)
    tanker_mmsis <<- unique(c(tanker_mmsis, new_mmsis))
    cat(sprintf("[REGISTRY] Tanker MMSI count: %d (added %d new)\n", 
                length(tanker_mmsis), length(new_mmsis)))
  }
  
  # ###########################################################
  # # (c) Hex-Filter Dynamic Messages by MMSI (from registry) #
  # ###########################################################
  
  # Get dynamic data only
  position_hex_lines <- hex_lines[is_position]
  
  # If dynamic data and registry are both non-empty
  if (length(position_hex_lines) > 0 && length(tanker_mmsis) > 0) {
    
    cat(sprintf("Filtering %d position messages by MMSI...\n", 
                length(position_hex_lines)))
    
    # Extract MMSI from hex and against tankers and filter by them
    position_mmsis <- sapply(position_hex_lines, extract_mmsi_hex)
    keep_position <- position_mmsis %in% tanker_mmsis
    position_hex_tankers <- position_hex_lines[keep_position]
    
    cat(sprintf("Kept %d tanker positions, filtered %d non-tankers\n",
                sum(keep_position), sum(!keep_position)))
    
    # ##############################################
    # # (d) Parse tanker dynamic/position messages #
    # ##############################################
    
    if (length(position_hex_tankers) > 0) {
      
      cat(sprintf("Parsing %d tanker position messages...\n", 
                  length(position_hex_tankers)))
      
      # Parse in parallel (6 cores)
      dynamic_list <- mclapply(position_hex_tankers, parse_dynamic, mc.cores = 6)
      dynamic_parse_errors <- sum(sapply(dynamic_list, is.null))  # Count NULLs
      dynamic_list <- rbindlist(dynamic_list[!sapply(dynamic_list, is.null)])
      
      # #############################################################
      # # (e) De-Duplication: Keep last message per MMSI per minute #
      # #############################################################
      
      if (nrow(dynamic_list) > 0) {
        cat(sprintf("Deduplicating %d messages...\n", nrow(dynamic_list)))
        
        # Round time to minute
        dynamic_list[, time_minute := as.POSIXct(format(time_utc, "%Y-%m-%d %H:%M:0S"), tz = "UTC")]
        
        # Sort by time (latest first)
        setorder(dynamic_list, mmsi, time_minute, -time_utc)
        
        # Keep first row per group (last chronologically)
        before_dedup <- nrow(dynamic_list)
        dynamic_list <- dynamic_list[, .SD[1], by = .(mmsi, time_minute)]
        after_dedup <- nrow(dynamic_list)
        
        # Remove helper column
        dynamic_list[, time_minute := NULL]
        
        cat(sprintf("Removed %d duplicates (kept last per MMSI per minute)\n",
                    before_dedup - after_dedup))
      }
    } else {
      dynamic_list <- data.table()
    }
  } else {
    dynamic_list <- data.table()
  }
  
  return(list(static = static_list, 
              dynamic = dynamic_list,
              stats_updates = list(
                hex_filtered_static = sum(is_static),
                hex_filtered_position = sum(is_position),
                hex_filtered_tankers = length(tanker_hex_lines),
                parsed_static_tankers = nrow(static_list),
                parsed_dynamic = if(exists("dynamic_list")) nrow(dynamic_list) else 0,
                dynamic_filtered_out = if(exists("keep_position")) sum(!keep_position) else 0,
                dynamic_kept = if(exists("after_dedup")) after_dedup else 0,
                dynamic_deduped = if(exists("before_dedup") && exists("after_dedup")) before_dedup - after_dedup else 0,
                parse_errors = static_parse_errors + dynamic_parse_errors 
              )))
}

# ##############################################################################
# # (5) Main Processing Loop -------------------------------------------------
# ##############################################################################

cat("------------------------------------------------------------------------------\n")
cat("Process 2: Parser & Filter\n")
cat(sprintf("[SYSTEM] Looking for files in: %s\n", raw_hex_dir))
cat(sprintf("[SYSTEM] Chunk size: %d lines\n", chunk_size))
cat("------------------------------------------------------------------------------\n\n")

# Get list of unprocessed raw hex files
get_unprocessed_files <- function() {
  all_files <- list.files(raw_hex_dir, pattern = "^raw_hex_.*\\.txt$", full.names = TRUE)
  
  # Only process files older than 5 seconds (not actively being written)
  stable_files <- all_files[vapply(all_files, function(f) {
    age <- difftime(Sys.time(), file.info(f)$mtime, units = "secs")
    # Force logical vector output
    as.numeric(age) > 5
  }, FUN.VALUE = logical(1))]
  
  stable_files[order(stable_files)]
}

# Process files in a loop
repeat {
  
  # Kill switch
  if (file.exists("STOP_AIS.txt")) {
    cat("\n[STOP] Stop file detected. Exiting Process 2.\n")
    break
  }
  
  # Get unprocessed files
  hex_files <- get_unprocessed_files()
  
  if (length(hex_files) == 0) {
    cat("[WAIT] No new hex files. Sleeping 10 seconds...\n")
    Sys.sleep(10)
    next
  }
  
  cat(sprintf("\n[FOUND] %d hex file(s) to process\n", length(hex_files)))
  
  # Process each file
  for (hex_file in hex_files) {
    cat(sprintf("\n[PROCESSING] %s\n", basename(hex_file)))
    
    processing_success <- TRUE 
    
    result <- tryCatch({
      
      # Open file connection
      con <- file(hex_file, "r") 
      
      # Storage for this file's data
      all_static <- list()
      all_dynamic <- list()
      
      # Read and process in chunks
      chunk_num <- 0
      repeat {
        # Read chunk (10,000 lines)
        hex_chunk <- tryCatch({
          readLines(con, n = chunk_size, warn = FALSE)
        }, error = function(e) {
          cat(sprintf("[ERROR] Failed to read chunk: %s\n", e$message))
          character(0)  
        })
       
        if (length(hex_chunk) == 0) break
        
        # Update chunk and message statistics
        chunk_num <- chunk_num + 1
        
        cat(sprintf("\n  [CHUNK %d] Processing %d lines...\n", chunk_num, length(hex_chunk)))
        
        # Process this chunk (with hex pre-filtering)
        processed <- process_hex_batch(hex_chunk)
        
        # Update stats
        stats_counter$hex_filtered_static <- stats_counter$hex_filtered_static + processed$stats_updates$hex_filtered_static
        stats_counter$hex_filtered_position <- stats_counter$hex_filtered_position + processed$stats_updates$hex_filtered_position
        stats_counter$hex_filtered_tankers <- stats_counter$hex_filtered_tankers + processed$stats_updates$hex_filtered_tankers
        stats_counter$parsed_static_tankers <- stats_counter$parsed_static_tankers + processed$stats_updates$parsed_static_tankers
        stats_counter$parsed_dynamic <- stats_counter$parsed_dynamic + processed$stats_updates$parsed_dynamic
        stats_counter$dynamic_filtered_out <- stats_counter$dynamic_filtered_out + processed$stats_updates$dynamic_filtered_out
        stats_counter$dynamic_kept <- stats_counter$dynamic_kept + processed$stats_updates$dynamic_kept
        stats_counter$dynamic_deduped <- stats_counter$dynamic_deduped + processed$stats_updates$dynamic_deduped
        stats_counter$parse_errors <- stats_counter$parse_errors + processed$stats_updates$parse_errors
        
        # Accumulate results
        if (nrow(processed$static) > 0) {
          all_static[[length(all_static) + 1]] <- processed$static
        }
        if (nrow(processed$dynamic) > 0) {
          all_dynamic[[length(all_dynamic) + 1]] <- processed$dynamic
        }
        
        # Memory management
        rm(hex_chunk, processed)
        gc(verbose = FALSE)
      }
      
      close(con)
      
      current_timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
      
      # Combine all chunks for this file
      if (length(all_static) > 0) {
        static_dt <- rbindlist(all_static)
        
        # Name static file
        static_out_file <- paste0(filtered_dir, "/filtered_static_", current_timestamp, ".rds")
        # Save filtered static data
        saveRDS(static_dt, static_out_file, compress = FALSE)
        cat(sprintf("\n[SAVED] Static: %d tanker records → %s\n", 
                    nrow(static_dt), basename(static_out_file)))
        # Update counter
        static_file_counter <- static_file_counter + 1
        
        rm(static_dt, all_static)
      }
      
      if (length(all_dynamic) > 0) {
        dynamic_dt <- rbindlist(all_dynamic)
        
        # Name dynamic file
        dynamic_out_file <- paste0(filtered_dir, "/filtered_dynamic_", current_timestamp, ".rds")
        
        # Save filtered dynamic data
        saveRDS(dynamic_dt, dynamic_out_file, compress = FALSE)
        cat(sprintf("[SAVED] Dynamic: %d tanker messages → %s\n", 
                    nrow(dynamic_dt), basename(dynamic_out_file)))
        
        # Update counter
        dynamic_file_counter <- dynamic_file_counter + 1
        
        rm(dynamic_dt, all_dynamic)
      }
      
      # Memory cleanup x2 for funsies
      gc(verbose = FALSE)
      
      # Return success with stats and counters 
      list(success = TRUE, 
           stats = stats_counter, 
           static_counter = static_file_counter, 
           dynamic_counter = dynamic_file_counter)
      
    }, error = function(e) {
      cat(sprintf("[ERROR] Failed to process %s: %s\n", basename(hex_file), e$message))
      
      # Log error
      write(sprintf("%s [PROCESS 2 ERROR] File: %s, Error: %s\n", 
                    Sys.time(), basename(hex_file), e$message),
            file = "logs/error_log.txt", append = TRUE)
      
      # Delete partial output if exists 
      # if (exists("static_out_file") && file.exists(static_out_file)) {
      #   file.remove(static_out_file)
      # }
      # if (exists("dynamic_out_file") && file.exists(dynamic_out_file)) {
      #   file.remove(dynamic_out_file)
      # }
      
      # Close connection if open
      if (exists("con") && isOpen(con)) {
        close(con)
      }
      
      # Return failure with current stats
      list(success = FALSE, 
           stats = stats_counter, 
           static_counter = static_file_counter,
           dynamic_counter = dynamic_file_counter)
    })
    
    # Update stats/counters
    stats_counter <- result$stats
    static_file_counter <- result$static_counter
    dynamic_file_counter <- result$dynamic_counter
    processing_success <- result$success
    
    # Handle file deletion based on success
    if (processing_success) {
      # Delete processed hex file
      file.remove(hex_file)
      cat(sprintf("[DELETED] %s (processing complete)\n", basename(hex_file)))
    } else {
      # Keep file for inspection on error
      cat(sprintf("[KEEP] %s - Manual intervention required\n", basename(hex_file)))
    }
  }
  
  # Print statistics
  cat("\n------------------------------------------------------------------------------\n")
  cat("PROCESS 2: Current Statistics (HEX-OPTIMIZED)\n")
  cat(sprintf("Total hex messages processed: %d\n", stats_counter$total_hex_messages))
  cat(sprintf("Hex filtered - Static: %d | Position: %d\n", 
              stats_counter$hex_filtered_static, stats_counter$hex_filtered_position))
  cat(sprintf("Hex filtered - Tankers: %d (Type 80-89)\n", stats_counter$hex_filtered_tankers))
  cat(sprintf("Parsed - Static tankers: %d | Dynamic: %d\n",
              stats_counter$parsed_static_tankers, stats_counter$parsed_dynamic))
  cat(sprintf("Dynamic - Kept: %d | Filtered: %d | Deduped: %d\n",
              stats_counter$dynamic_kept, stats_counter$dynamic_filtered_out, stats_counter$dynamic_deduped))
  cat(sprintf("Tanker MMSI registry size: %d\n", length(tanker_mmsis)))
  cat(sprintf("Parse errors: %d\n", stats_counter$parse_errors))
  
  # Calculate speedup estimate
  total_processed <- stats_counter$hex_filtered_static + stats_counter$hex_filtered_position
  actually_parsed <- stats_counter$parsed_static_tankers + stats_counter$parsed_dynamic
  if (total_processed > 0) {
    speedup_ratio <- total_processed / max(actually_parsed, 1)
    cat(sprintf("Hex optimization: Avoided parsing %d messages (%.1fx speedup)\n",
                total_processed - actually_parsed, speedup_ratio))
  }
  cat("------------------------------------------------------------------------------\n\n")
  
  # Save tanker MMSI registry"
  saveRDS(tanker_mmsis, registry_file)
  cat(sprintf("[REGISTRY] Saved to %s\n\n", registry_file))
  
  # Wait before checking for new files
  Sys.sleep(5)
}

cat("[SYSTEM] Process 2 terminated.\n")

