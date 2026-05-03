# #############################################################################
# # Process 3: CPU-Intensive Work                                             #
# #############################################################################

# #############################################################################
# # (0) Initialize  --------------------------------------------------------- #
# #############################################################################

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(dbscan)
  library(mclust)
  library(geosphere)
  library(lubridate)
  library(parallel)
})

# #############################################################################
# # (1) Configuration --------------------------------------------------------
# #############################################################################

# Directories
filtered_dir <- "data/filtered"   # Get from
processed_dir <- "data/processed" # Print to
if (!dir.exists(processed_dir)) dir.create(processed_dir, recursive = TRUE)
if (!dir.exists("logs")) dir.create("logs")

# File counter
processed_file_counter <- 1

# Timing parameters
cycle_interval_mins <- 1 * 24 * 60  # in minutes -> every/24 hours

# Statistics
stats_count <- list(
  files_processed = 0,
  static_records = 0,
  dynamic_records = 0,
  draught_events_detected = 0,
  berthing_events_detected = 0,
  dbscan_vessels_processed = 0,
  dbscan_outliers_removed = 0
)

# #############################################################################
# # (2) Lookup Table Setup ----------------------------------------------
# #############################################################################

# #####################
# # Port-lookup table #
# #####################

ports_df <- fread('data/World Port Index/UpdatedPub150.csv',
                  select = c(
                    "OID_", 
                    "Main Port Name", 
                    "Country Code",
                    "Latitude",
                    "Longitude")
) |>
  mutate(
    port_name = paste0(`Main Port Name`, ", ", `Country Code`)
  ) |>
  select(
    port_name, 
    lat = Latitude,
    lon = Longitude
  )

# Port-lookup shapefile table
ports_sf <- st_as_sf(ports_df, coords = c("lon", "lat"), crs = 4326)

# ############################################################################
# # MARAD Dimensions Lookup - # Papanikolaou "Ship Design Methodologies of   #
# # Preliminary Design: https://doi.org/10.1007/978-94-017-8751-2            #
# ############################################################################

MARAD <- data.table(
  parameter = LETTERS[1:16],
  length_beam = c(5.5, 6, 6.5, 4.5, 5, 5.5, 5, 6.5, 6, 6, 5, 5, 5.5, 5, 5.5, 5),
  beam_draught = c(3, 3, 3, 3, 3, 3, 3, 3, 3.75, 4.5, 3.75, 4.5, 3.75, 3.75, 3.75, 4.5),
  middle_c = rep(0.994, 16),
  block_c = c(0.875, 0.875, 0.875, 0.850, 0.850, 0.850, 0.800, 0.850, 
              0.850, 0.850, 0.850, 0.850, 0.875, 0.800, 0.875, 0.800)
)
setkey(MARAD, length_beam, beam_draught)

# #############################################################################
# # Vessel classification lookup: https://porteconomicsmanagement.org/pemp/c- #
# # ontents/part5/ports-and-energy/tanker-size/                               #
# # https://www.eia.gov/todayinenergy/detail.php?id=17991                     #
# #############################################################################

vessel_class_lookup <- data.table(
  min_dwt = c(0, 10000, 25000, 45000, 80000, 120000, 200000, 320000),
  max_dwt = c(10000, 25000, 45000, 80000, 120000, 200000, 320000, Inf),
  commercial_class = c("Small Tanker", "Handysize", "Handymax", "Panamax", 
                       "Aframax", "Suezmax", "VLCC", "ULCC"),
  technical_class = c("ST", "HS", "HM", "PM", "AF", "SZ", "VLCC", "ULCC"),
  primary_cargo = c("Refined", "Refined", "Refined/Crude", "Refined/Crude",
                    "Crude", "Crude", "Crude", "Crude")
)
setkey(vessel_class_lookup, min_dwt, max_dwt)

# #############################################################################
# # (3) Core Processing Functions --------------------------------------------
# #############################################################################

# AIS data-driven analysis for identifying cargo handling events in international 
# trade tankers. Zhang et al. (2025)

# ###########################
# # Clean AIS data function #
# ###########################

clean_ais_data <- function(static, dynamic) {
  
  
  
  # Clean static data 
  static_na <- static[is.na(draught) | draught <= 0 | is.na(mmsi)]
  static <- static[!is.na(draught) & 
                     draught > 0 & 
                     !is.na(lat) & 
                     !is.na(lon) & 
                     !is.na(length_m) & 
                     !is.na(breadth_m) & 
                     !is.na(mmsi), ]
  
  # DBSCAN clustering to remove draught outliers - used earlier than Zhang et al.
  # to reduce data - fulfills same job
  if (nrow(static) > 2) {
    
    # Process each MMSI separately
    static_clean <- static[, {
      # Only run DBSCAN if the vessel has enough data points
      if (.N > 2) {
        # Subset draught dbscan
        db_result <- dbscan(as.matrix(.SD$draught), eps = 2, minPts = 2)
        # Keep core and border points, not noise
        .SD[db_result$cluster != 0]
      } else {
        .SD
      }
    }, by = mmsi]
    
    setDT(static_clean)
    
    # Calculate outliers removed
    dbscan_outliers <- static[!static_clean, on = names(static)]
    
    # Update statistics
    vessels_processed <- uniqueN(static$mmsi)
    stats_count$dbscan_vessels_processed <<- stats_count$dbscan_vessels_processed + vessels_processed
    stats_count$dbscan_outliers_removed <<- stats_count$dbscan_outliers_removed + nrow(dbscan_outliers)
    
    cat(sprintf("[DBSCAN] Processed %d vessels: kept %d, removed %d outliers\n",
                vessels_processed, nrow(static_clean), nrow(dbscan_outliers)))
  } else { 
    static_clean <- static
    dbscan_outliers <- data.table()
  }
  
  # Clean dynamic data (remove invalid coordinates and speeds)
  dynamic_invalid <- dynamic[is.na(lat) |
                               is.na(lon) |
                               is.na(sog) |
                               lat < -90 |
                               lat > 90 | 
                               lon < -180 |
                               lon > 180 | 
                               sog < 0 | 
                               sog > 40 | 
                               is.na(mmsi) |
                               is.na(cog) |
                               cog < 0 | 
                               cog > 360, ]
  dynamic <- dynamic[!is.na(lat) & 
                       !is.na(lon) & 
                       !is.na(sog) & 
                       lat >= -90 & 
                       lat <= 90 &
                       lon >= -180 & 
                       lon <= 180 & 
                       sog >= 0 & 
                       sog <= 40 & 
                       !is.na(mmsi) &
                       !is.na(cog) & 
                       cog >= 0 & 
                       cog <= 360, ]
 
  # Clean cog data - shelve it for now
  minute_threshold <- 6
  dynamic[, cog := {
    time_diff_prev <- as.numeric(difftime(time_utc, shift(time_utc, 1), units = "mins"))
    time_diff_next <- as.numeric(difftime(shift(time_utc, -1), time_utc, units = "mins"))
    cog_diff_prev <- abs((cog - shift(cog, 1) + 180) %% 360 - 180)
    cog_diff_next <- abs((cog - shift(cog, -1) + 180) %% 360 - 180)
    # If a large change in cog occurs over a short time
    fifelse(time_diff_prev < minute_threshold &
              time_diff_next < minute_threshold &
              (cog_diff_prev > 30 & cog_diff_next > 30),
            # Replace with average of adjacent trajectory points
            (shift(cog, 1) + shift(cog, -1))/2,
            # Else cog
            cog)
  }, by = mmsi]
  # Remove NA or invalid cogs
  dynamic_clean <- dynamic
  if (nrow(dynamic_invalid) == 0) {dynamic_invalid <- data.table()}
  
  return(list(
    static_ls = list(static_clean, dbscan_outliers, static_na),
    dynamic_ls = list(dynamic_clean, dynamic_invalid)
  ))
}

# #########################################################
# # Detect draught changes and calculate cargo quantities #
# #########################################################

detect_draught_changes <- function(static) {

  setDT(static)
  
  # Sort by vessel and time
  setorder(static, mmsi, time_utc)
  
  # Initialize columns
  static[, `:=`(
    date_seconds = NA_real_,
    prev_date_seconds = NA_real_,
    draught_diff = NA_real_,
    time_diff_hours = NA_real_,
    distance_km = NA_real_,
    prev_lat = NA_real_,
    prev_lon = NA_real_
  )]
  
  # Add seconds timestamp
  static[, date_seconds := as.numeric(time_utc)]
  
  # Remove repetitive data
  static[, change := draught != shift(draught, 1), by = mmsi]
  static <- static[change == TRUE | shift(change, 1, type = "lead") == TRUE]
  
  # Calculate draught differences
  static[, draught_diff := draught - shift(draught, 1), by = mmsi]
  static[, prev_date_seconds := shift(date_seconds, 1), by = mmsi]
  
  # Calculate time difference between draught changes
  static[, time_diff_hours := (date_seconds - prev_date_seconds) / 3600]
  
  # Setup for Haversine distance
  static[, `:=`(prev_lat = shift(lat, 1), prev_lon = shift(lon, 1)), by = mmsi]
  
  # Only calculate distance where we have valid coordinate pairs
  valid_coords_idx <- which(!is.na(static$prev_lat) & !is.na(static$lat))
  if (length(valid_coords_idx) > 0) {
    coords_prev <- static[valid_coords_idx, .(prev_lon, prev_lat)]
    coords_curr <- static[valid_coords_idx, .(lon, lat)]
    # Update only the valid rows (distance travelled in kms)
    static[valid_coords_idx, distance_km := distHaversine(coords_prev, coords_curr) / 1000]
  }
  
  draught_change_small <- static[abs(draught_diff) <= 1 & abs(draught_diff) > 0]
  draught_no_change <- static[draught_diff == 0]
  
  # Filter thresholds: >1m change, >12h time gap, >10km distance
  events <- static[abs(draught_diff) > 1 &
                   time_diff_hours > 12 &
                   distance_km > 12]
  
  events_rm <- static[!(abs(draught_diff)) > 1 &
                      time_diff_hours <= 12 &
                      distance_km <= 12]
  
  cat(sprintf("[DRAUGHT] Detected %d significant draught change events\n", nrow(events)))
  
  # Estimate DWT from length (equation estimated in dwt_estimation.R)
  events[, est_dwt := -971.2256 + 1.0273 * length_m * breadth_m * draught]
  # Estimate draught from Kalokairinos et al. (2000-2005) in Papanikolaou's "Ship Design" (2014) (p. 474)
  events[, design_draught := 0.45011 * (est_dwt ^ 0.303134)]
  # Calculate length/beam and beam/draught ratios
  events[, length_beam := round(length_m / breadth_m * 2) / 2] # .5 increments 
  # max(draught) needs to be design draught instead
  events[, beam_draught := round(breadth_m / design_draught * 4) / 4] # .25 increments
  
  marad_errors <- events[!MARAD, on = .(length_beam, beam_draught)]
  setkey(MARAD, length_beam, beam_draught)
  
  # Join with MARAD lookup table
  events <- MARAD[events, 
                  .(mmsi, 
                    imo = i.imo,
                    ship_name = i.ship_name,
                    time_utc, 
                    date_seconds, 
                    prev_date_seconds,
                    draught, 
                    draught_diff, 
                    length_m,
                    breadth_m, 
                    est_dwt,
                    design_draught,
                    beam_draught = i.beam_draught,
                    length_beam = i.length_beam,
                    # If block_c is NA (no match), use 0.85
                    block_c = fcoalesce(block_c, 0.850), 
                    # If middle_c is NA (no match), use 0.994
                    middle_c = fcoalesce(middle_c, 0.994)),
                  on = .(length_beam, beam_draught),
                  roll = "nearest",
                  mult = "first"] # if matches to multiple MARAD categories, picks the first match
  
  setDT(events)

  # Calculate waterplane area coefficients and cargo quantities:
  # Schneekluth and Bertram "Ship Design for Efficiency and Economy", p. 11
  events[, waterplane_c := (1 + 2 * (block_c / middle_c)) / 3]
  
  # Tonnes per centimeter immersion estimates = waterplane area * saltwater density * 100
  # Zhang et. al.
  events[, tpci := length_m * breadth_m * waterplane_c * 1.025 / 100]
  # DWT change (tonnes) = TPCI * draught_diff (m) * 100 (cm/m conversion)
  events[, dwt_change := tpci * draught_diff * 100]
  # Absolute mass (tonnes)
  events[, abs_mass := abs(dwt_change)]
  # Calculate displacement/variable lightweight in Prakash and Smith's "Estimating vessel payloads in bulk shipping using AIS Data" 
  events[, displacement := length_m * breadth_m * design_draught * block_c * 1.025, by = mmsi]
  # Estimate Lightship (only ship weight, metric tonnes) using regression equation from Papanikolaou, A. (2014). Ship Design: Methodologies of Preliminary Design (p. 459). Springer.
  events[, lightship_tonnage := 2.9186 * (displacement ^ 0.75548)]
  # Estimate light ballast meters
  events[, light_ballast_m := 2.0 + 0.02 * length_m]
  # Estimate the light ballast in metric tonnes
  events[, light_ballast_tonnes := (tpci * 100 * light_ballast_m) - lightship_tonnage]
  # Estimate heavy ballast in metric tonnes
  events[, heavy_ballast_tonnes := est_dwt * 0.05, by = mmsi]
  # Calculate change in ballast in metric tonnes
  events[, ballast_change_t := light_ballast_tonnes - heavy_ballast_tonnes]
  # Calculate change in cargo in barrels of oil = change in deadweight tonnage + change in ballast
  events[, cargo_tonnes_change := abs_mass + ballast_change_t]
  # Convert cargo into barrels
  events[, cargo_barrels_change := cargo_tonnes_change * 7.3]
  # Classify event type
  events[, event_type := fifelse(draught_diff > 0, "loading", "level")]
  events[draught_diff < 0, event_type := "unloading"]
  
  # Define time windows for extracting dynamic trajectory segments
  events[, window_start := prev_date_seconds]
  events[, window_end := date_seconds]
  events[, window_start_posix := as.POSIXct(window_start, origin = "1970-01-01", tz = "UTC")]
  events[, window_end_posix := as.POSIXct(window_end, origin = "1970-01-01", tz = "UTC")]
  
  return(list(clean = events, missing = list(draught_change_small,
                                             draught_no_change,
                                             events_rm,
                                             marad_errors)))
}

# ########################################
# # Assign port names using spatial join #
# ########################################

assign_nearest_port <- function(dt, lon_col, lat_col, result_col) {
  dt_sf <- st_as_sf(copy(dt), coords = c(lon_col, lat_col), crs = 4326)
  nearest_idx <- st_nearest_feature(dt_sf, ports_sf)
  dt[[result_col]] <- ports_df$port_name[nearest_idx]
  dt[is.na(get(result_col)), (result_col) := "Lost at Sea"]
  return(dt)
}

assign_port_names <- function(dt) {
  dt <- assign_nearest_port(dt, "prev_lon", "prev_lat", "start_port")
  dt <- assign_nearest_port(dt, "lon", "lat", "end_port")
  return(dt)
}

# ######################################
# # GMM-based berthing event detection #
# ######################################

find_berthing_event <- function(segment) {
  
  # Ensure enough data
  # Zhang et al. uses 10 here
  if (is.null(segment) || nrow(segment) < 3) {
    return(list(success = FALSE, data = segment))
  }
  
  # Ensure we have required columns
  if (!"sog" %in% names(segment)) {
    cat("[ERROR] No sog column in segment\n")
    return(NULL)
  }
  
  # If all speeds are the same, GMM cannot calculate variance
  # Need enough observations
  sog_clean <- segment$sog[!is.na(segment$sog)]
  if (length(sog_clean) < 3 || length(unique(sog_clean)) < 2) {
    return(list(success = FALSE, data = segment))
  }
  
  # Copy to local variable to avoid scoping issues
  local_segment <- copy(segment)
  
  # Check for problem sog data
  if (all(is.na(local_segment$sog))) return(NULL)
  if (length(unique(local_segment$sog[!is.na(local_segment$sog)])) < 2) return(NULL)
  if (var(local_segment$sog, na.rm = TRUE) == 0) return(NULL)
  
  # Fit GMM to Speed Over Ground (2 components: moored vs moving)
  model <- tryCatch(
    suppressWarnings(Mclust(local_segment$sog, G = 2, verbose = FALSE)), 
    error = function(e) NULL
  )
  if (is.null(model)) return(list(success = FALSE, data = segment))
  
  # Extract parameters
  means <- model$parameters$mean
  moored_mean_id <- which.min(means)
  
  # Threshold = μ + 3σ (captures 99.7% of moored state)
  threshold_sog <- means[moored_mean_id] + 
    3 * sqrt(model$parameters$variance$sigmasq[moored_mean_id])
  # Single value cases
  current_threshold <- if (length(threshold_sog) > 1) threshold_sog[moored_mean_id] else threshold_sog
  
  # Calculate minimum trajectory count threshold
  # t_max <- max(local_segment$date_seconds)
  # t_min <- min(local_segment$date_seconds)
  # delta_num <- (t_max - t_min) / 360  # AIS update rate: 6 min = 360 sec
  
  # Filter for berthing points (low speed)
  setorder(local_segment, time_utc)
  berthing_points <- local_segment[sog <= threshold_sog]
  
  # berthing_logical <- local_segment$sog <= threshold_sog
  # berthing_points <- segment[berthing_logical]
  
  # if (length(berthing_logical) != nrow(local_segment)) {
  #   cat("[ERROR] Length mismatch detected!\n")
  #   return(NULL)
  # }
  
  if (nrow(berthing_points) >= 2) { 
    return(list(success = TRUE, data = berthing_points)) # Added success flag
  } else {
    return(list(success = FALSE, data = segment)) # Return the full segment as 'missing'
  }
  
  # Validate minimum duration
  # if (nrow(berthing_points) > delta_num) {
  #   return(berthing_points)
  # } else {
  #   return(NULL)
  # }
}

###############################################
# # Run complete pipeline for a batch of data #
###############################################

run_zhang_pipeline <- function(static, dynamic) {

  cat("[PIPELINE] Starting Zhang et al. (2025) cargo event detection...\n")
  
  # Clean data
  cleaned_list <- clean_ais_data(static = static, dynamic = dynamic)
  static_clean <- cleaned_list$static_ls[[1]]
  dynamic_clean <- cleaned_list$dynamic_ls[[1]]
  dbscan_outliers <- cleaned_list$static_ls[[2]]
  dynamic_error <- cleaned_list$dynamic_ls[[2]]
  static_error <- cleaned_list$static_ls[[3]]
  
  # Detect draught change events
  draught_event_ls <- detect_draught_changes(static_clean)
  draught_events <- draught_event_ls$clean
  draught_error_small <- draught_event_ls$missing[[1]]
  draught_error_none <- draught_event_ls$missing[[2]]
  draught_error <- draught_event_ls$missing[[3]]
  marad_error <- draught_event_ls$missing[[4]]

  if (nrow(draught_events) == 0) {
    cat("[PIPELINE] No draught events detected. Skipping berthing analysis.\n")
    return(list(final = data.table(), 
                missing = list(
                  static_error, 
                  dbscan_outliers,
                  dynamic_error, 
                  draught_error,
                  draught_error_small,
                  draught_error_none,
                  marad_error)))
  }
  
  # Assign port names to dynamic data
  dynamic_clean[, date_seconds := as.numeric(time_utc)]
  events_cl <- assign_port_names(dynamic_clean, port_dt)
  
  # Synchronize dynamic trajectory with draught events
  setnames(draught_events, "time_utc", "static_event_time")
  
  # Join dynamic data (events_cl) with port names to static data (draught_events) - remove
  synchronized_events <- events_cl[draught_events, 
                                   on = .(mmsi, 
                                          time_utc >= window_start_posix,
                                          time_utc <= window_end_posix),
                                   allow.cartesian = TRUE]

  cat(sprintf("[SYNC] Matched %d dynamic points to %d draught events\n",
              nrow(synchronized_events), nrow(draught_events)))
  
  gmm_errors <- list()
  
  # Find true berthing events using GMM
  results_dt <- synchronized_events[, {
    segment <- .SD # subset to current column
    
    # Progress tracking - pinging
    if (.GRP %% 5 == 0) {  # Every 5th vessel
      cat(sprintf("\r[GMM] Processing %d...", .GRP))
    }
    
    # Validate required columns
    if (!all(c("sog", "time_utc", "date_seconds") %in% names(segment))) {
      cat(sprintf("[ERROR] Missing columns in .SD for group mmsi=%d\n", mmsi[1]))
      return(NULL)
    }
    
    gmm_check <- find_berthing_event(segment)
    
    if (!is.null(gmm_check) && gmm_check$success) {
      # If true, extract berthing points
      true_event <- gmm_check$data
      .(
        imo = imo[1],
        event_type = event_type[1],
        vessel_name = ship_name[1], 
        length_m = length_m[1],
        breadth_m = breadth_m[1],
        draught_change_m = draught_diff[1],
        draught_end_m = draught[1],
        est_mass_mt = dwt_change[1],
        abs_mass = round(abs_mass[1], 0),
        design_draught = design_draught[1],
        displacement = displacement[1],
        dwt = est_dwt[1],
        lightship_tonnage = lightship_tonnage[1],
        light_ballast_m = light_ballast_m[1],
        light_ballast_tonnes = light_ballast_tonnes[1],
        heavy_ballast_tonnes = heavy_ballast_tonnes[1],
        waterplane_coefficient = waterplane_c[1],
        tons_per_centimeter_immersion = tpci[1],
        ballast_change_tonnes = ballast_change_t[1],
        cargo_change_tonnes = cargo_tonnes_change[1],
        cargo_change_barrels = cargo_barrels_change[1],
        # From dynamic data
        start_time = min(true_event$time_utc),
        end_time = max(true_event$time_utc),
        port_name = true_event$port_name[1], 
        start_lat = true_event$lat[which.min(true_event$time_utc)],
        start_lon = true_event$lon[which.min(true_event$time_utc)],
        end_lat = true_event$lat[which.max(true_event$time_utc)],
        end_lon = true_event$lon[which.max(true_event$time_utc)]
      )
    } else {
      error_key <- paste0(mmsi[1], "_", as.character(static_event_time[1]))
      gmm_errors[[error_key]] <<- copy(segment)
      # Exclude this event from "final" table
      NULL
    }
    # One cargo event per group
  }, by = .(mmsi, static_event_time)]
  
  # Turn into a dt for later
  gmm_error_table <- rbindlist(gmm_errors, fill = TRUE)
  
  cat(sprintf("[GMM] Detected %d berthing events\n", nrow(results_dt)))
  
  class_lookup_errors <- results_dt[!vessel_class_lookup,
                                    on = .(abs_mass >= min_dwt, abs_mass <= max_dwt)]
  
  # Classify vessels by cargo capacity
  final_results <- vessel_class_lookup[results_dt, 
                                       on = .(min_dwt <= abs_mass, max_dwt >= abs_mass),
                                       .(mmsi, 
                                         imo,
                                         event_type, 
                                         vessel_name,
                                         port_name,
                                         length_m,
                                         breadth_m,
                                         draught_change_m,
                                         draught_end_m,
                                         est_mass_mt, 
                                         abs_mass,
                                         design_draught,
                                         displacement,
                                         dwt,
                                         lightship_tonnage,
                                         light_ballast_m,
                                         light_ballast_tonnes,
                                         heavy_ballast_tonnes,
                                         waterplane_coefficient,
                                         tons_per_centimeter_immersion,
                                         ballast_change_tonnes,
                                         cargo_change_tonnes,
                                         cargo_change_barrels,
                                         commercial_class, 
                                         technical_class, 
                                         primary_cargo, 
                                         start_time,
                                         end_time,
                                         start_lat,
                                         start_lon,
                                         end_lat,
                                         end_lon)]
  
  final_results[is.na(commercial_class), commercial_class := "Unclassified"]
  
  cat(sprintf("[COMPLETE] Pipeline finished. %d cargo events identified.\n", 
              nrow(final_results)))
  
  return(list(
    final = final_results, 
    missing = list(
      static = static_error, 
      dbscan = dbscan_outliers,
      dynamic = dynamic_error, 
      draught = draught_error,
      draught_small = draught_error_small,
      draught_none = draught_error_none,
      marad = marad_error,
      gmm = gmm_error_table,
      class_lookup = class_lookup_errors)
  ))
}

# #############################################################################
# # (4) Main Processing Loop -------------------------------------------------
# #############################################################################
cat("------------------------------------------------------------------------------\n")
cat("Process 3: CPU-Intensive Work\n")
cat(sprintf("Work cycle in: %d minutes", cycle_interval_mins))
cat(sprintf("Kill switch: Create STOP_AIS.txt file\n"))
cat("------------------------------------------------------------------------------\n\n")

# Get unprocessed filtered files 
get_unprocessed_filtered_files <- function() {
  static_files <- list.files(filtered_dir, 
                             pattern = "^filtered_static_.*\\.rds$", 
                             full.names = TRUE)
  dynamic_files <- list.files(filtered_dir, 
                              pattern = "^filtered_dynamic_.*\\.rds$", 
                              full.names = TRUE)
  
  list(static = static_files, dynamic = dynamic_files)
}

# Track when next cycle runs
last_cycle_time <- Sys.time() - (cycle_interval_mins * 60) # turns to minutes

# Main work cycle
repeat {
  current_timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  
  # Kill switch
  if (file.exists("STOP_AIS.txt")) {
    cat("\n[STOP] Stop file detected. Exiting Process 3.\n")
    break
  }
  
  # Calculate time until next daily cycle
  time_since_last_cycle <- as.numeric(difftime(Sys.time(), last_cycle_time, units = "secs"))
  cycle_interval_secs <- cycle_interval_mins * 60
  
  # If enough time has passed, run the processing cycle
  if (time_since_last_cycle >= cycle_interval_secs) {
    
    # Track when this cycle starts
    cycle_start_time <- Sys.time() 
    cat(sprintf("\n[CYCLE START] Beginning processing cycle at %s\n", Sys.time()))
    
    # Get files to process
    files <- get_unprocessed_filtered_files()
    cat(sprintf("\n[FOUND] %d static file(s), %d dynamic file(s)\n",
                length(files$static), length(files$dynamic)))
    
    # Load all static data
    all_static <- rbindlist(lapply(files$static, function(f) {
      tryCatch({
        dt <- readRDS(f)
      }, error = function(e) {
        write(sprintf("%s [LOAD ERROR] %s: %s\n", Sys.time(), basename(f), e$message),
              file = "logs/error_log.log", append = TRUE)
        return(data.table())
      })
    }))
    
    # Load all dynamic data
    all_dynamic <- rbindlist(lapply(files$dynamic, function(f) {
      tryCatch({
        dt <- readRDS(f)
      }, error = function(e) {
        write(sprintf("%s [LOAD ERROR] %s: %s\n", Sys.time(), basename(f), e$message),
              file = "logs/error_log.log", append = TRUE)
        return(data.table())
      })
    }))
    cat(sprintf("[LOADED] Static: %d records, Dynamic: %d records\n",
                nrow(all_static), nrow(all_dynamic)))
    
    # Update statistics
    stats_count$static_records <- stats_count$static_records + nrow(all_static)
    stats_count$dynamic_records <- stats_count$dynamic_records + nrow(all_dynamic)
    
    # Run Zhang pipeline if files are nonempty
    if (nrow(all_static) > 0 && nrow(all_dynamic) > 0) {
      
      tryCatch({
        results <- run_zhang_pipeline(static = all_static, dynamic = all_dynamic)
        
        # Save processed results
        processed_out_file <- paste0(processed_dir, "/processed_", current_timestamp, ".rds")
        saveRDS(results, processed_out_file, compress = FALSE)
        
        cat(sprintf("[SAVED] Processed results → %s\n", basename(processed_out_file)))
        
        # Update statistics
        stats_count$files_processed <- stats_count$files_processed + 1
        stats_count$draught_events_detected <- stats_count$draught_events_detected + 
          nrow(results$final)
        
        processed_file_counter <- processed_file_counter + 1
        
        # Name daily aggregator file
        if (!dir.exists("data/output")) dir.create("data/output", recursive = TRUE)
        master_file <- "data/output/all_cargo_events.rds"
        
        # Track initial size
        initial_size <- if (file.exists(master_file)) file.size(master_file) else 0
        
        # Get new data to append
        new_data <- results$final[((imo != 0 & !is.na(imo)) | !is.na(mmsi)) & !is.na(start_time)]
        new_data <- unique(new_data)
        
        # Append to master file
        if (file.exists(master_file)) {
          existing_data <- readRDS(master_file)
          combined_data <- rbindlist(list(existing_data, new_data), fill = TRUE)
          combined_data[, time_block := round_date(start_time, "4 hours")]
          combined_data <- unique(combined_data, by = c("mmsi", "time_block"))
          combined_data[, time_block := NULL]
          setorder(combined_data, mmsi, start_time)
          saveRDS(combined_data, file = master_file)
          cat(sprintf("[AGGREGATION] Appended %d events (total: %d)\n", 
                      nrow(new_data), nrow(combined_data)))
        } else {
          setorder(new_data, mmsi, start_time)
          saveRDS(new_data, file = master_file)
          cat(sprintf("[AGGREGATION] Created master file with %d events\n", nrow(new_data)))
        }
        
        # Check if file size increased
        final_size <- file.size(master_file)

        # Get all processed files
        all_processed_files <- list.files(processed_dir, pattern = "^processed_.*\\.rds$", full.names = TRUE)

        if (length(all_processed_files) > 1 && final_size > initial_size) {
          # Get file modification times
          file_times <- file.mtime(all_processed_files)
          # Find most recent file by date time
          most_recent <- all_processed_files[which.max(file_times)]

          # Remove all except most recent
          to_remove <- all_processed_files[all_processed_files != most_recent]
          file.remove(to_remove)
          cat(sprintf("[CLEANUP] Removed %d old processed files, kept most recent\n", length(to_remove)))
        }
        
        # Archive filtered files after 60 hours
        all_files <- c(files$static, files$dynamic)
        # Extract time stamps for both file types
        timestamps <- regmatches(all_files, regexpr("\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}", all_files))
        timestamps <- as.POSIXct(timestamps, format = "%Y-%m-%d_%H-%M-%S")
        # Create a logical vector to filter by (remove files > 24 hours old)
        delete_ls <- timestamps < (Sys.time() - 60 * 60 * 12)
        # Filter by logical vector
        if (any(delete_ls, na.rm = TRUE)) {
          # Create archive
          if (!dir.exists("data/archived")) dir.create("data/archived", recursive = TRUE)
          # Move to archive
          archived_count <- 0
          for (i in which(delete_ls)) {
            old_path <- all_files[i]
            new_path <- gsub("data/filtered", "data/archived", old_path)
            
            if (file.rename(old_path, new_path)) {
              archived_count <- archived_count + 1
            } else {
              cat(sprintf("[WARN] Failed to archive: %s\n", basename(old_path)))
            }
          }
          cat(sprintf("\n[CLEANUP] Archived %d old files.\n", sum(delete_ls)))
        }
      }, error = function(e) {
        cat(sprintf("[ERROR] Pipeline failed: %s\n", e$message))
        write(sprintf("%s [PIPELINE ERROR] %s\n", Sys.time(), e$message),
              file = "logs/error_log.log", append = TRUE)
      })
    }
    
    cycle_duration <- as.numeric(difftime(Sys.time(), cycle_start_time, units = "secs"))
    cat("\n------------------------------------------------------------------------------\n")
    cat("PROCESS 3: Cycle Complete\n")
    cat(sprintf("Cycle duration: %.1f seconds\n", cycle_duration))
    cat(sprintf("Files processed: %d\n", stats_count$files_processed))
    cat(sprintf("Static records: %d\n", stats_count$static_records))
    cat(sprintf("Dynamic records: %d\n", stats_count$dynamic_records))
    cat(sprintf("Draught events detected: %d\n", stats_count$draught_events_detected))
    cat(sprintf("Next cycle in: %d minutes\n", cycle_interval_mins))
    cat("------------------------------------------------------------------------------\n\n")
    
    rm(list = ls(pattern = "^(all_static|all_dynamic|results)$"))
    gc(verbose = FALSE)
    
    # Update last cycle time
    last_cycle_time <- Sys.time()
    
  } else {
    
    # Wait before checking again
    time_remaining <- cycle_interval_secs - time_since_last_cycle
    cat(sprintf("\r[WAITING] Next cycle in %.0f seconds...", time_remaining), 
        file = stderr())
    Sys.sleep(60)  
    
  }
  if (file.exists("STOP_AIS.txt")) break
}

cat("[SYSTEM] Process 3 terminated.\n")





