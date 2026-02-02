# #############################################################################
# SETUP SCRIPT: AIS Stream Processing System
# #############################################################################
# Purpose: Create directory structure, check dependencies, verify configuration
# Run this BEFORE launching the system

cat("------------------------------------------------------------------------------\n")
cat("AIS STREAM PROCESSING SYSTEM - SETUP\n")
cat("------------------------------------------------------------------------------\n\n")

# #############################################################################
# # (1) Create Directory Structure -------------------------------------------
# #############################################################################

cat("[STEP 1] Creating directory structure\n")

required_dirs <- c(
  "data",
  "data/raw_hex",
  "data/filtered", 
  "data/processed",
  "data/output",
  "logs",
  "logs/processes",
  "files"
)

# Create folders if they don't exist already
for (dir in required_dirs) {
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
    cat(sprintf("[CREATED] %s\n", dir))
  } else {
    cat(sprintf("[EXISTS] %s\n", dir))
  }
}

cat("\n")

# #############################################################################
# # (2) Check Required Packages ----------------------------------------------
# #############################################################################
cat("[STEP 2] Checking required packages\n")

required_packages <- c(
  "websocket",      # Process 1: WebSocket connection
  "jsonlite",       # Process 1,2: JSON parsing
  "data.table",     # Process 2,3,4: Fast data manipulation
  "dplyr",          # Process 2,3,4: Data wrangling
  "dbscan",         # Process 3: Outlier detection
  "mclust",         # Process 3: GMM clustering
  "geosphere",      # Process 3: Haversine distances
  "lubridate",      # Process 3,4: Date/time handling
  "processx",       # Launcher: Process management
  "parallel",       # Process 2,3: Parallel processing
  "later"           # Process 1: Event loop
)

missing_packages <- character()

for (pkg in required_packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    cat(sprintf("[MISSING] %s\n", pkg))
    missing_packages <- c(missing_packages, pkg)
  } else {
    cat(sprintf("[OK] %s\n", pkg))
  }
}

if (length(missing_packages) > 0) {
  cat("\n[ACTION REQUIRED] Install missing packages:\n")
  cat(sprintf("install.packages(c(%s))\n", paste0('"', missing_packages, '"', collapse = ", ")))
  cat("\n")
} else {
  cat("\n[SUCCESS] All required packages are installed\n\n")
}

# #############################################################################
# # (3) Check API Key --------------------------------------------------------
# #############################################################################

cat("[STEP 3] Checking API key configuration\n")

api_key_file <- "files/ais_api_key.R"

if (!file.exists(api_key_file)) {
  cat(sprintf("  [MISSING] %s\n", api_key_file))
  cat("\n[ACTION REQUIRED] Create API key file\n")
} else {
  # Try to load the API key
  tryCatch({
    test_key <- source(api_key_file)$value
    if (is.null(test_key) || nchar(test_key) < 10) {
      cat("[WARNING] API key file exists but may be invalid\n")
    } else {
      cat(sprintf("[OK] API key loaded (length: %d characters)\n", nchar(test_key)))
    }
  }, error = function(e) {
    cat("[ERROR] Could not load API key:", e$message, "\n")
  })
  cat("\n")
}

# #############################################################################
# # (4) Verify Process Scripts -----------------------------------------------
# #############################################################################

cat("[STEP 4] Verifying process scripts\n")

process_scripts <- c(
  "process_1_websocket_receiver.R",
  "process_2_parser_filter.R",
  "process_3_cpu_intensive.R",
  "background_launcher.R"
)

all_scripts_present <- TRUE

for (script in process_scripts) {
  if (file.exists(file.path("files", script))) {
    cat(sprintf("[OK] %s\n", script))
  } else {
    cat(sprintf("[MISSING] %s\n", script))
    all_scripts_present <- FALSE
  }
}

if (!all_scripts_present) {
  cat("\n[ERROR] Some process scripts are missing\n\n")
} else {
  cat("\n[SUCCESS] All process scripts found\n\n")
}

# #############################################################################
# # (5) Create Test Stop File (then delete) ---------------------------------
# #############################################################################

cat("[STEP 5] Testing kill switch\n")

test_stop_file <- "STOP_AIS.txt"

# Create test file
tryCatch({
  file.create(test_stop_file)
  cat("[OK] Kill switch test file created\n")
  
  # Verify we can detect it
  if (file.exists(test_stop_file)) {
    cat("[OK] Kill switch file detected\n")
    
    # Delete it
    file.remove(test_stop_file)
    cat("[OK] Kill switch file removed\n")
  }
}, error = function(e) {
  cat("[ERROR] Kill switch test failed:", e$message, "\n")
})

