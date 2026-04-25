#!/usr/bin/env Rscript
# setwd("/AIS Tanker Tracker")
# #############################################################################
# # Smart Launcher - WiFi Check + Auto-Restart                               #
# #############################################################################

# #############################################################################
# # Configuration -----------------------------------------------------------
# #############################################################################

# Process definitions
process_scripts <- c(
  "files/process_1_websocket_receiver.R",
  "files/process_2_parser_filter.R", 
  "files/process_3_cpu_intensive.R",
  "files/process_4_archive_to_postgreSQL.R"
)

# Monitoring settings
check_interval_secs <- 300   # 5 minutes 
launch_delay_secs <- 15      

# Log directory
log_dir <- "logs/processes"
if (!dir.exists(log_dir)) dir.create(log_dir, recursive = TRUE)

# #############################################################################
# # Helper Functions --------------------------------------------------------
# #############################################################################

# Check WiFi connectivity
is_wifi_connected <- function() {
  result <- tryCatch({
    # Ping Google DNS (8.8.8.8) with 1 packet, 2-second timeout
    ping_result <- system2("ping", 
                           args = c("-c", "1", "-W", "2000", "8.8.8.8"), 
                           stdout = FALSE, 
                           stderr = FALSE)
    # Exit code 0 = success
    return(ping_result == 0)
  }, error = function(e) {
    return(FALSE)
  })
  
  return(result)
}

# Check if all processes are running
are_processes_running <- function() {
  tryCatch({
    if (.Platform$OS.type == "unix") {
      # Count processes matching our script names
      ps_output <- system("ps aux | grep -E 'process_[1-4]' | grep -v grep | wc -l", 
                          intern = TRUE)
      count <- as.numeric(trimws(ps_output))
      # All 4 processes should be running
      return(count >= 4)
    } 
  }, error = function(e) {
    cat(sprintf("[ERROR] Could not check process status: %s\n", e$message))
    return(FALSE)
  })
}

# Kill all existing AIS processes
kill_existing_processes <- function() {
  cat("[CLEANUP] Stopping existing processes...\n")
  
  if (.Platform$OS.type == "unix") {
    # Kill by pattern matching
    system("pkill -f 'process_1_websocket'", ignore.stdout = TRUE, ignore.stderr = TRUE)
    system("pkill -f 'process_2_parser'", ignore.stdout = TRUE, ignore.stderr = TRUE)
    system("pkill -f 'process_3_cpu'", ignore.stdout = TRUE, ignore.stderr = TRUE)
    system("pkill -f 'process_4_archive'", ignore.stdout = TRUE, ignore.stderr = TRUE)
    
    # Give processes time to clean up
    Sys.sleep(2)
    
    # Verify they're stopped
    still_running <- system("ps aux | grep -E 'process_[1-4]' | grep -v grep | wc -l",
                            intern = TRUE)
    if (as.numeric(trimws(still_running)) == 0) {
    } else {
      cat("[CLEANUP] Processes still running\n")
    }
  }
}

# Launch all processes
launch_all_processes <- function() {
  cat("\n[LAUNCH] Starting all processes...\n")
  
  # First, clean up any existing processes
  kill_existing_processes()
  
  # For each script
  for (i in seq_along(process_scripts)) {
    script_path <- process_scripts[i]
    
    # Check if script exists
    if (!file.exists(script_path)) {
      cat(sprintf("[ERROR] Script not found: %s\n", script_path))
      next
    }
    
    # Create timestamped log file
    timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
    log_file <- file.path(log_dir, sprintf("process_%d_%s.log", i, timestamp))
    
    # Launch process in background
    if (.Platform$OS.type == "unix") {
      cmd <- sprintf("nohup Rscript '%s' > '%s' 2>&1 &", script_path, log_file)
      system(cmd)
    } 
    
    cat(sprintf("[LAUNCH] Process %d: %s\n", i, basename(script_path)))
    cat(sprintf("         Log: %s\n", log_file))
    
    # Delay between launches to avoid resource spike
    Sys.sleep(launch_delay_secs)
  }
  
  cat("[LAUNCH] All processes started\n")
  cat(sprintf("[INFO] Monitor logs: tail -f %s/*.log\n", log_dir))
}

# #############################################################################
# # Main Loop ---------------------------------------------------------------
# #############################################################################

cat("------------------------------------------------------------------------------\n")
cat("Smart Launcher - WiFi Check + Auto-Restart\n")
cat(sprintf("Monitoring interval: %d seconds (%.1f minutes)\n", 
            check_interval_secs, check_interval_secs/60))
cat(sprintf("Process launch delay: %d seconds\n", launch_delay_secs))
cat(sprintf("Log directory: %s\n", log_dir))
cat(sprintf("\nKill switch: Create 'STOP_AIS.txt' file in project root\n"))
cat("------------------------------------------------------------------------------\n\n")

# Clean up any existing processes on start
kill_existing_processes()

# Track whether processes have been launched
processes_launched <- FALSE

# Main monitoring loop
repeat {
  
  # Check for kill switch
  if (file.exists("STOP_AIS.txt")) {
    cat("\n[STOP] Stop file detected. Shutting down...\n")
    kill_existing_processes()
    # Remove stop file for next run
    tryCatch({
      file.remove("STOP_AIS.txt")
      cat("[STOP] Removed STOP_AIS.txt\n")
    }, error = function(e) {
      cat("[STOP] Could not remove STOP_AIS.txt\n")
    })
    break
  }
  
  # Check WiFi connectivity
  wifi_connected <- is_wifi_connected()
  
  if (!wifi_connected) {
    cat(sprintf("[%s] No WiFi connection - waiting...\n", 
                format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
    
    # If processes are running but no WiFi, let them continue
    # (they might be using cached data or will handle disconnection)
    if (processes_launched) {
      cat("[INFO] Processes running without WiFi (they will handle reconnection)\n")
    }
    
  } else {
    # WiFi is connected - check process status
    if (!processes_launched || !are_processes_running()) {
      
      if (!processes_launched) {
        cat(sprintf("[%s] WiFi connected - launching processes...\n", 
                    format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
      } else {
        cat(sprintf("[%s] Processes crashed - relaunching...\n", 
                    format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
      }
      
      # Launch all processes
      launch_all_processes()
      processes_launched <- TRUE
      
    } else {
      # Everything running normally
      next_check <- Sys.time() + check_interval_secs
      cat(sprintf("[%s] ✓ All systems operational (next check: %s)\n", 
                  format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
                  format(next_check, "%H:%M:%S")))
    }
  }
  
  # Sleep until next check
  Sys.sleep(check_interval_secs)
}

cat("\n----------------------------------------------------------------------------\n")
cat("[EXIT] Launcher terminated\n")
cat("------------------------------------------------------------------------------\n")