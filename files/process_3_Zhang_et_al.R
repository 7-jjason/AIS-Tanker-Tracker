# #############################################################################
# # Process 3: CPU-Intensive Work                                             #
# #############################################################################

# Notes:
# (1) Track statistics entire way through so they dont cause issues x
# (2) Optimize parameter selection in practice.
# (3) Integrate another AIS WebSocket with process 1.
# (4) 

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
  library(sf)
})

# #############################################################################
# # (1) Configuration --------------------------------------------------------
# #############################################################################

# Directories
filtered_dir <- "data/archived"   # Get from
processed_dir <- "data/processed" # Print to
if (!dir.exists(processed_dir)) dir.create(processed_dir, recursive = TRUE)
if (!dir.exists("logs")) dir.create("logs")

# Track processed files
# processed_files_tracker <- character()

# File counter
processed_file_counter <- 1

# Timing parameters
cycle_interval_mins <- 1 * 24 * 60  # Minutes

# Cumulative stats
stats_total <- list(
  cycles_ran = 0,
  static_records = 0,
  dynamic_records = 0,
  load_errors = 0,
  pipeline_errors = 0,
  draught_events_cumulative = 0
)

# Per-cycle stats - reset each cycle
stats_count <- list(
  draught_events_detected_pre_gmm = 0,
  draught_events_detected = 0,
  berthing_events_detected = 0,
  dbscan_vessels_processed = 0,
  static_outliers_removed = 0,
  static_na_removed = 0,
  static_ab_removed = 0,
  dynamic_na_removed = 0,
  dynamic_ab_removed = 0,
  cog_corrections_made = 0,
  distance_filter_removed = 0,
  static_dedup_removed = 0,
  dynamic_dedup_removed = 0,
  draught_small_change = 0,
  draught_no_change = 0,
  draught_threshold_removed = 0,
  marad_lookup_misses = 0,
  gmm_failures = 0,
  gmm_null_returns = 0,
  traj_points_filter_removed= 0
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

# ##########################################################################
# # MARAD Dimensions Lookup - # Papanikolaou "Ship Design Methodologies of #
# # Preliminary Design: https://doi.org/10.1007/978-94-017-8751-2          #
# ##########################################################################

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
# # (3) Core Processing Functions --------------------------------------------
# #############################################################################

# AIS data-driven analysis for identifying cargo handling events in international 
# trade tankers. Zhang et al. (2025)

# ###########################
# # Clean AIS data function #
# ###########################

# Input: static and dynamic datasets.
# Transformation: filters them according to Zhang et al.
# Output: static list containing cleaned static data, outliers removed via 
#         DBSCAN, and NA data; dynamic list containing cleaned dynamic data and 
#         invalid data.

clean_ais_data <- function(static, dynamic) {

  # Invalid data cleaning #
  
  # Static NA removal
  static_na <- static[is.na(draught) | 
                        is.na(mmsi) | 
                        is.na(lat) | 
                        is.na(lon), ]
  static <- static[!is.na(draught) &
                     !is.na(mmsi) &
                     !is.na(lat) & 
                     !is.na(lon), ]
  # Update statistics
  stats_count$static_na_removed <<- stats_count$static_na_removed + nrow(static_na)
  
  # Dynamic NA removal
  dynamic_na <- dynamic[is.na(lat) |
                          is.na(lon) |
                          is.na(mmsi) |
                          is.na(sog) |
                          is.na(cog), ]
  dynamic <- dynamic[!is.na(lat) & 
                       !is.na(lon) & 
                       !is.na(mmsi) &
                       !is.na(sog) & 
                       !is.na(cog), ]
  stats_count$dynamic_na_removed <<- stats_count$dynamic_na_removed + nrow(dynamic_na)
  
  # Abnormal/anomalous data preprocessing #
  
  # Static abnormal data removal
  static_ab <- static[draught <= 0 |
                        lat < -90 | 
                        lat > 90 |
                        lon < -180 | 
                        lon > 180, ]
  static <- static[draught > 0 &
                     lat >= -90 & 
                     lat <= 90 &
                     lon >= -180 & 
                     lon <= 180, ]
  stats_count$static_ab_removed <<- stats_count$static_ab_removed + nrow(static_ab)
  
  # DBSCAN clustering to remove draught outliers
  if (nrow(static) > 2) {
    # Process each MMSI separately
    static_clean <- static[, {
      if (.N > 2) {
        draught_vals <- .SD$draught
        valid_ind    <- which(!is.na(draught_vals))
        
        if (length(valid_ind) > 2) {
          db_result  <- dbscan(as.matrix(draught_vals[valid_ind]), eps = 1, minPts = 2)
          keep_ind   <- valid_ind[db_result$cluster != 0]
          .SD[keep_ind]
        } else {
          .SD
        }
      } else {
        .SD
      }
    }, by = mmsi]
    
    setDT(static_clean)
    
    # Track outliers removed
    static_outliers <- static[!static_clean, on = names(static)]
    # Update statistics
    vessels_processed <- uniqueN(static$mmsi)
    stats_count$dbscan_vessels_processed <<- stats_count$dbscan_vessels_processed + vessels_processed
    stats_count$static_outliers_removed <<- stats_count$static_outliers_removed + nrow(static_outliers)
    cat(sprintf("[DBSCAN] Processed %d vessels, removed %d outliers\n",
                vessels_processed, nrow(static_outliers)))
    
  } else { # if DBSCAN isn't successful
    # pass static
    static_clean <- static
    # return empty
    static_outliers <- data.table()
  }
  # Dynamic abnormal data removal
  dynamic_ab <- dynamic[lat < -90 | 
                          lat > 90 |
                          lon < -180 | 
                          lon > 180 | 
                          sog < 0 | 
                          sog > 40 | 
                          cog < 0 | 
                          cog > 360, ]
  dynamic <- dynamic[lat >= -90 & 
                       lat <= 90 &
                       lon >= -180 & 
                       lon <= 180 & 
                       sog >= 0 & 
                       sog <= 40 & 
                       cog >= 0 & 
                       cog <= 360, ]
  stats_count$dynamic_ab_removed <<- stats_count$dynamic_ab_removed + nrow(dynamic_ab)
  # Clean course over ground (COG) data 
  minute_threshold <- 6
  cog_threshold <- 30
  dynamic[, `:=`(
    time_diff_prev = as.numeric(difftime(time_utc, shift(time_utc, 1), units = "mins")),
    time_diff_next = as.numeric(difftime(shift(time_utc, -1), time_utc, units = "mins"))
  )]
  # Update statistics
  cog_correction_mask <- dynamic[, {
    cog_diff_prev <- abs((cog - shift(cog, 1) + 180) %% 360 - 180)
    cog_diff_next <- abs((cog - shift(cog, -1) + 180) %% 360 - 180)
    .(corrected = sum(
      time_diff_prev < minute_threshold &
      time_diff_next < minute_threshold &
      cog_diff_prev > cog_threshold &
      cog_diff_next > cog_threshold,
      na.rm = TRUE
    ))
  }, by = mmsi]
  stats_count$cog_corrections_made <<- stats_count$cog_corrections_made + sum(cog_correction_mask$corrected)
  # Filter COG
  dynamic_clean <- dynamic[, cog := {
    cog_diff_prev <- abs((cog - shift(cog, 1) + 180) %% 360 - 180)
    cog_diff_next <- abs((cog - shift(cog, -1) + 180) %% 360 - 180)
    # If a large change in cog occurs over a short time
    fifelse(time_diff_prev < minute_threshold &
              time_diff_next < minute_threshold &
              (cog_diff_prev > cog_threshold & cog_diff_next > cog_threshold),
            # Replace with average of adjacent trajectory points
            (shift(cog, 1) + shift(cog, -1))/2,
            # Else cog
            cog)
  }, by = mmsi]
  
  # Prep haversine distance calculation
  dynamic_clean[, `:=`(prev_lat = shift(lat, 1), 
                       prev_lon = shift(lon, 1)), by = mmsi]
  # Only calculate distance where we have valid coordinate pairs
  valid_coords_idx <- which(!is.na(dynamic_clean$prev_lat) & !is.na(dynamic_clean$lat))
  # Calculate haversine distance
  if (length(valid_coords_idx) > 0) {
    coords_prev <- dynamic_clean[valid_coords_idx, .(prev_lon, prev_lat)]
    coords_curr <- dynamic_clean[valid_coords_idx, .(lon, lat)]
    # Update only the valid rows (distance travelled in kms)
    dynamic_clean[valid_coords_idx, distance_km := distHaversine(coords_prev, coords_curr) / 1000]
  }
  # Calculate sog threshold
  dynamic_clean[, sog_threshold := max(sog, na.rm = TRUE) * time_diff_prev, by = mmsi]
  # Count rows pre-filter for statistics
  n_before_distance_filter <- nrow(dynamic_clean)
  # Filter positiond data
  dynamic_clean <- dynamic_clean[distance_km <= sog_threshold,]
  # Update statistics 
  stats_count$distance_filter_removed <<- stats_count$distance_filter_removed + (n_before_distance_filter - nrow(dynamic_clean))
  
  # Field Conversion #
  # Add seconds timestamp 
  static_clean[, date_seconds := as.numeric(time_utc)]
  dynamic_clean[, date_seconds := as.numeric(time_utc)]
  
  # Data Deduplication #
  
  # Count data for statistics
  n_before_static_dedup <- nrow(static_clean)
  n_before_dynamic_dedup <- nrow(dynamic_clean)
  
  static_clean[, `:=` (
    lat_check = round(lat, 1),
    lon_check = round(lon, 1),
    date_seconds_check = round(date_seconds/10, 0)
  )]
  # distinct instead?
  static_clean <- unique(static_clean, by = c("mmsi", "lat_check", "lon_check", "draught", "date_seconds_check"))
  static_clean[, `:=` (lat_check = NULL, lon_check = NULL, date_seconds_check = NULL)]
  stats_count$static_dedup_removed <<- stats_count$static_dedup_removed + (n_before_static_dedup - nrow(static_clean))

  dynamic_clean[, `:=` (
    lat_check = round(lat, 1),
    lon_check = round(lon, 1),
    date_seconds_check = round(date_seconds/10, 0)
  )]
  dynamic_clean <- unique(dynamic_clean, by = c("mmsi", "lat", "lon", "date_seconds_check"))
  dynamic_clean[, `:=` (lat_check = NULL, lon_check = NULL, date_seconds_check = NULL)]
  stats_count$dynamic_dedup_removed <<- stats_count$dynamic_dedup_removed + (n_before_dynamic_dedup - nrow(dynamic_clean))
  
  # Track data removed - i could just do this instead of the granular tracking and for static too
  static_errors <- static[!static_clean, on = names(static)]
  if (nrow(static_errors) == 0) {static_errors <- data.table()}
  dynamic_errors <- dynamic[!dynamic_clean, on = names(dynamic)]
  if (nrow(dynamic_errors) == 0) {dynamic_errors <- data.table()}

  
  return(list(
    static_ls = list(static_clean = static_clean, 
                     static_na = static_na, 
                     static_ab = static_ab, 
                     static_outliers = static_outliers),
    dynamic_ls = list(dynamic_clean = dynamic_clean, 
                      dynamic_na = dynamic_na, 
                      dynamic_ab = dynamic_ab, 
                      dynamic_errors = dynamic_errors)
  ))
}

# #########################################################
# # Detect draught changes and calculate cargo quantities #
# #########################################################

# Input: static dataset.
# Transformation: finds hydrostatic coefficients via a MARAD lookup table, 
#                 calculates tons per centimeter immersion (TPCI), and change
#                 in deadweight tonnage.
# Output: list of cleaned draught events; list of removed data.

detect_draught_changes <- function(static) {
  
  setDT(static)
  
  # Sort by vessel and time
  setorder(static, mmsi, time_utc)
  
  # Initialize columns
  static[, `:=`(
    prev_date_seconds = NA_real_,
    draught_diff = NA_real_,
    time_diff_hours = NA_real_,
    distance_km = NA_real_,
    prev_lat = NA_real_,
    prev_lon = NA_real_
  )]
  
  # Remove repetitive data
  static[, change := draught != shift(draught, 1), by = mmsi]
  # Filter to keep where changes occur
  static <- static[change == TRUE | shift(change, 1, type = "lead") == TRUE, by = mmsi]
  
  # Calculate draught differences
  static[, draught_diff := draught - shift(draught, 1), by = mmsi]
  
  # Calculate time difference between draught changes
  static[, prev_date_seconds := shift(date_seconds, 1), by = mmsi]
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
  stats_count$draught_small_change <<- stats_count$draught_small_change + nrow(draught_change_small)
  
  draught_no_change <- static[draught_diff == 0]
  stats_count$draught_no_change <<- stats_count$draught_no_change + nrow(draught_no_change)
  
  # Filter thresholds: >1m change, >12h time gap, >10km distance
  draught_threshold <- 1
  time_threshold <- 1
  distance_threshold <- 1
  
  events <- static[abs(draught_diff) > draught_threshold &
                     time_diff_hours > time_threshold &
                     distance_km > distance_threshold]
  events_rm <- static[!(abs(draught_diff) > draught_threshold) & 
                        time_diff_hours <= time_threshold &
                        distance_km <= distance_threshold]
  
  stats_count$draught_threshold_removed <<- stats_count$draught_threshold_removed + nrow(events_rm)
  stats_count$draught_events_detected_pre_gmm <<- stats_count$draught_events_detected_pre_gmm + nrow(events)
  
  cat(sprintf("[DRAUGHT] Detected %d significant draught change events\n", nrow(events)))
  
  # Calculate length/beam and beam/draught ratios to find hydrostatic coefficients
  events[, `:=` (
    length_beam = round(length_m / breadth_m * 2) / 2, # .5 increments 
    beam_draught = round(breadth_m / max(draught, na.rm = TRUE) * 4) / 4 # .25 increments
  ), by = mmsi]
  
  marad_errors <- events[!MARAD, on = .(length_beam, beam_draught)]
  stats_count$marad_lookup_misses <<- stats_count$marad_lookup_misses + nrow(marad_errors)
  setkey(MARAD, length_beam, beam_draught)
  
  # Join with MARAD lookup table to retrieve associated hydrostatic block and middle coefficients
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
                    beam_draught = i.beam_draught,
                    length_beam = i.length_beam,
                    block_c = fcoalesce(block_c, 0.850), # If block_c is NA (no match), use 0.85
                    middle_c = fcoalesce(middle_c, 0.994)), # If middle_c is NA (no match), use 0.994
                  on = .(length_beam, beam_draught),
                  roll = "nearest",
                  mult = "first"] # if matches to multiple MARAD categories, picks the first match
  
  setDT(events)
  
  # Hydrostatic waterplane area coefficient - Schneekluth and Bertram "Ship Design for Efficiency and Economy", p. 11
  events[, waterplane_c := (1 + 2 * (block_c / middle_c)) / 3]
  # Tonnes per centimeter immersion estimates = waterplane area * saltwater density / 100 
  events[, tpci := length_m * breadth_m * waterplane_c * 1.025 / 100]
  # DWT change (metric tonnes) - Zhang et. al.
  events[, dwt_change := tpci * draught_diff * 100] # *100*100/10000
  # In ten thousands of barrels - Zhang et. al
  events[, dwt_change_Mbbl := tpci * draught_diff * 7] # *100*100/10000
  events[, abs_mass := abs(dwt_change)]
  
  # Classify event type
  events[, event_type := fifelse(dwt_change >= 1, "loading", "level")]
  events[dwt_change <= -1, event_type := "unloading"]
  
  # Define time windows for extracting dynamic trajectory segments
  events[, window_start := as.POSIXct(prev_date_seconds, origin = "1970-01-01", tz = "UTC")]
  events[, window_end := as.POSIXct(date_seconds, origin = "1970-01-01", tz = "UTC")]
  
  return(list(clean = events, missing = list(draught_change_small = draught_change_small,
                                             draught_no_change = draught_no_change,
                                             events_rm = events_rm,
                                             marad_errors = marad_errors)))
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
  if (is.null(segment) || nrow(segment) < 3) {
    return(list(success = FALSE, data = segment))
  }
  
  # Ensure we have required columns
  if (!"sog" %in% names(segment)) {
    cat("[ERROR] No sog column in segment\n")
    return(NULL)
  }
  
  # If all speeds are the same, GMM cannot calculate variance - need enough observations
  sog_clean <- segment$sog[!is.na(segment$sog)]
  if (length(sog_clean) < 3 || length(unique(sog_clean)) < 2) {
    return(list(success = FALSE, data = segment))
  }
  
  # Avoid scoping issues
  local_segment <- copy(segment)
  
  # Check for problem sog data
  if (all(is.na(local_segment$sog))) return(NULL)
  if (length(unique(local_segment$sog[!is.na(local_segment$sog)])) < 2) return(NULL)
  if (var(local_segment$sog, na.rm = TRUE) == 0) return(NULL)
  
  # Fit GMM to Speed Over Ground (2 components - moored vs moving)
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
  # current_threshold <- if (length(threshold_sog) > 1) threshold_sog[moored_mean_id] else threshold_sog
  
  # Filter for berthing points
  setorder(local_segment, time_utc)
  berthing_points <- local_segment[sog <= threshold_sog]
  
  if (nrow(berthing_points) >= 2) { 
    return(list(success = TRUE, data = berthing_points)) 
  } else {
    return(list(success = FALSE, data = segment)) 
  }
}

###############################################
# # Run complete pipeline for a batch of data #
###############################################

run_zhang_pipeline <- function(static, dynamic) {
  
  cat("[PIPELINE] Starting Zhang et al. (2025) cargo event detection...\n")
  
  # Clean data
  cleaned_list <- clean_ais_data(static = static, dynamic = dynamic)
  static_clean <- cleaned_list$static_ls[["static_clean"]]
  static_na <- cleaned_list$static_ls[["static_na"]]
  static_ab <- cleaned_list$static_ls[["static_ab"]]
  static_outlier <- cleaned_list$static_ls[["static_outliers"]]
  
  dynamic_clean <- cleaned_list$dynamic_ls[["dynamic_clean"]]
  dynamic_na <- cleaned_list$dynamic_ls[["dynamic_na"]]
  dynamic_ab <- cleaned_list$dynamic_ls[["dynamic_ab"]]
  dynamic_errors <- cleaned_list$dynamic_ls[["dynamic_errors"]]
  
  # Detect draught change events
  draught_event_ls <- detect_draught_changes(static_clean)
  draught_events <- draught_event_ls$clean
  draught_error_small <- draught_event_ls$missing[["draught_change_small"]]
  draught_error_static <- draught_event_ls$missing[["draught_no_change"]]
  draught_rm <- draught_event_ls$missing[["events_rm"]]
  marad_error <- draught_event_ls$missing[["marad_errors"]]
  
  if (nrow(draught_events) == 0) {
    cat("[PIPELINE] No draught events detected. Skipping berthing analysis.\n")
    return(list(final = data.table(), 
                missing = list(
                  static_na, 
                  static_ab,
                  static_outlier,
                  dynamic_na, 
                  dynamic_ab,
                  dynamic_errors,
                  draught_error_small,
                  draught_error_static,
                  draught_rm,
                  marad_error)))
  }
  
  # check draught events before this to see if draught_events has two utc times
  # Create new columns cause they disappear
  draught_events[,`:=` (
    start_bound = window_start,
    end_bound = window_end
  )]
  # Change names for join
  setnames(draught_events, 
           old = c("time_utc", "date_seconds"),
           new = c("time_utc_draught", "date_seconds_draught"))
  setnames(dynamic_clean,
           "ship_name",
           "ship_name_1")
  # Join dynamic (events_cl) with static data (draught_events) 
  # how 
  synchronized_events <- dynamic_clean[draught_events, 
                                       on = .(mmsi, 
                                              time_utc >= start_bound,
                                              time_utc <= end_bound),
                                       allow.cartesian = TRUE]
  
  synchronized_events[, ship_name_1 := NULL]
  # okay to lose silently
  synchronized_unmatched <- synchronized_events[!complete.cases(synchronized_events), ]
  synchronized_events <- na.omit(synchronized_events)
  
  cat(sprintf("[SYNC] Matched %d dynamic points to %d draught events\n",
              nrow(synchronized_events), nrow(draught_events)))
  
  gmm_errors <- list()
  
  # Find berthing events using GMM
  final_results <- synchronized_events[, {
    segment <- .SD # subset to current mmsi
    
    gmm_check <- find_berthing_event(segment)
    
    if (!is.null(gmm_check) && gmm_check$success) {
      # If true, extract berthing points
      true_event <- gmm_check$data
      .(
        imo = imo[1],
        ship_name = ship_name[1], 
        window_start = window_start[1],
        window_end = window_end[1],
        event_type = event_type[1],
        length_m = length_m[1],
        breadth_m = breadth_m[1],
        draught_m = draught[1],
        draught_change_m = draught_diff[1],
        block_coefficient = block_c[1], 
        middle_coefficient = middle_c[1], 
        waterplane_coefficient = waterplane_c[1],
        tons_per_centimeter_immersion = tpci[1],
        est_tonnes = dwt_change[1],
        est_Mbbl = dwt_change_Mbbl[1],
        abs_mass = abs_mass[1],
        prev_lat = prev_lat[1],
        prev_lon = prev_lon[1],
        lat = lat[1],
        lon = lon[1],
        distance_km = distance_km[1], # why is distance so small
        nav_stat = nav_stat[1]
      )
    } else {
      error_key <- paste0(mmsi[1], "_", as.character(time_utc_draught[1]))
      gmm_errors[[error_key]] <<- copy(segment)
      stats_count$gmm_failures <<- stats_count$gmm_failures + 1
      NULL
    }
  }, by = .(mmsi)]

  gmm_error_table <- rbindlist(gmm_errors, fill = TRUE)
  
  cat(sprintf("[GMM] Detected %d berthing events\n", nrow(final_results)))
  stats_count$berthing_events_detected <<- stats_count$berthing_events_detected + nrow(final_results)
  
  # Filter for enough trajectory points
  final_results[, min_traj_points := (as.numeric(window_end) - as.numeric(window_start))/360, by = mmsi]
  n_before_traj_filter <- nrow(final_results)
  final_results_c <- copy(final_results)
  final_results <- final_results[, if (.N >= min_traj_points[1]) .SD, by = mmsi]
  stats_count$traj_points_filter_removed <<- stats_count$traj_points_filter_removed + (n_before_traj_filter - nrow(final_results))
  # Track errors
  traj_rm <- final_results_c[!final_results, on = "mmsi"]
  
  final_results <- assign_port_names(final_results)
  
  cat(sprintf("[COMPLETE] Pipeline finished. %d cargo events identified.\n", 
              nrow(final_results)))
  
  return(list(
    final = final_results, 
    missing = list(
      static_na = static_na, 
      static_abnormal = static_ab,
      static_outlier = static_outlier,
      dynamic_na = dynamic_na,
      dynamic_abnormal = dynamic_ab,
      dynamic_errors = dynamic_errors,
      draught_rm = dynamic_errors,
      draught_error_small = draught_error_small,
      draught_error_static = draught_error_static,
      marad_error = marad_error,
      gmm = gmm_error_table,
      traj_rm = traj_rm,
      synchronized_unmatched = synchronized_unmatched)
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

# remove eventually
standardize_to_breadth <- function(dt) {
  # Force all column names to lowercase first to avoid breadth_m/BREADTH_M issues
  setnames(dt, tolower(names(dt)))
  # Rename beam_m or width_m to breadth_m if they exist
  if ("beam_m" %in% names(dt)) setnames(dt, "beam_m", "breadth_m")
  if ("width_m" %in% names(dt)) setnames(dt, "width_m", "breadth_m")
  return(dt)
}

# Track when next cycle runs
last_cycle_time <- Sys.time() - (cycle_interval_mins * 60)

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
        # remove eventually
        standardize_to_breadth(dt)  # Standardize each file individually
      }, error = function(e) {
        write(sprintf("%s [LOAD ERROR] %s: %s\n", Sys.time(), basename(f), e$message),
              file = "logs/error_log.log", append = TRUE)
        stats_total$load_errors <<- stats_total$load_errors + 1
        return(data.table())
      })
    }))
    
    # Load all dynamic data
    all_dynamic <- rbindlist(lapply(files$dynamic, function(f) {
      tryCatch({
        dt <- readRDS(f)
        # remove eventually
        standardize_to_breadth(dt)  # Standardize each file individually
      }, error = function(e) {
        write(sprintf("%s [LOAD ERROR] %s: %s\n", Sys.time(), basename(f), e$message),
              file = "logs/error_log.log", append = TRUE)
        stats_total$load_errors <<- stats_total$load_errors + 1
        return(data.table())
      })
    }))
    cat(sprintf("[LOADED] Static: %d records, Dynamic: %d records\n",
                nrow(all_static), nrow(all_dynamic)))
    
    # Update statistics
    stats_total$static_records <- stats_total$static_records + nrow(all_static)
    stats_total$dynamic_records <- stats_total$dynamic_records + nrow(all_dynamic)
    
    # Run Zhang pipeline if files are nonempty
    if (nrow(all_static) > 0 && nrow(all_dynamic) > 0) {
      
      tryCatch({
        results <- run_zhang_pipeline(static = all_static, dynamic = all_dynamic)
        
        # Save processed results
        processed_out_file <- paste0(processed_dir, "/processed_", current_timestamp, ".rds")
        saveRDS(results, processed_out_file, compress = FALSE)
        
        cat(sprintf("[SAVED] Processed results → %s\n", basename(processed_out_file)))
        
        # Update statistics
        stats_total$cycles_ran <- stats_total$cycles_ran + 1
        stats_count$draught_events_detected <- stats_count$draught_events_detected + 
          nrow(results$final)
        stats_total$draught_events_cumulative <- stats_total$draught_events_cumulative + nrow(results$final)
        
        processed_file_counter <- processed_file_counter + 1
        
        # Name daily aggregator file
        if (!dir.exists("data/output")) dir.create("data/output", recursive = TRUE)
        master_file <- "data/output/all_cargo_events.rds"
        
        # Track initial size
        initial_size <- if (file.exists(master_file)) file.size(master_file) else 0
        
        # Get new data to append
        new_data <- results$final[!is.na(mmsi) & !is.na(window_start) & !is.na(window_end) & !is.na(est_tonnes)]
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
        timestamps <- as.POSIXct(timestamps, format = "%Y-%m-%d_%H:%M:%S")
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
        stats_total$pipeline_errors <<- stats_total$pipeline_errors + 1
      })
    }
    
    cycle_duration <- as.numeric(difftime(Sys.time(), cycle_start_time, units = "secs"))
    
    total_input <- stats_total$static_records + stats_total$dynamic_records
    total_removed <- stats_count$static_na_removed +
      stats_count$static_ab_removed +
      stats_count$static_outliers_removed +
      stats_count$static_dedup_removed +
      stats_count$dynamic_na_removed +
      stats_count$dynamic_ab_removed +
      stats_count$distance_filter_removed +
      stats_count$dynamic_dedup_removed +
      stats_count$draught_small_change +
      stats_count$draught_no_change +
      stats_count$draught_threshold_removed +
      stats_count$traj_points_filter_removed
    
    cat("\n------------------------------------------------------------------------------\n")
    
    cat("PROCESS 3: Cycle Complete\n")
    
    cat(sprintf("  Cycle duration: %.1f seconds\n", cycle_duration))
    cat(sprintf("  Cycles ran: %d\n", stats_total$cycles_ran))
    cat(sprintf("  Draught events cumulative: %d\n", stats_total$draught_events_cumulative))
    cat(sprintf("  Observations removed: %d\n", total_removed))
    cat(sprintf("  Static records: %d\n", stats_total$static_records))
    cat(sprintf("  Dynamic records: %d\n", stats_total$dynamic_records))
    cat(sprintf("  Draught events detected pre-GMM: %d\n", stats_count$draught_events_detected_pre_gmm))
    cat(sprintf("  Draught events detected: %d\n", stats_count$draught_events_detected))
    cat(sprintf("  Next cycle in: %d minutes\n", cycle_interval_mins))
    cat(sprintf("  Static NA removed:          %d\n", stats_count$static_na_removed))
    cat(sprintf("  Static abnormal removed:    %d\n", stats_count$static_ab_removed))
    cat(sprintf("  Static DBSCAN outliers:     %d\n", stats_count$static_outliers_removed))
    cat(sprintf("  Static dedup removed:       %d\n", stats_count$static_dedup_removed))
    cat(sprintf("  Dynamic NA removed:         %d\n", stats_count$dynamic_na_removed))
    cat(sprintf("  Dynamic abnormal removed:   %d\n", stats_count$dynamic_ab_removed))
    cat(sprintf("  COG corrections made:       %d\n", stats_count$cog_corrections_made))
    cat(sprintf("  Distance filter removed:    %d\n", stats_count$distance_filter_removed))
    cat(sprintf("  Dynamic dedup removed:      %d\n", stats_count$dynamic_dedup_removed))
    cat(sprintf("  Draught small change:       %d\n", stats_count$draught_small_change))
    cat(sprintf("  Draught no change:          %d\n", stats_count$draught_no_change))
    cat(sprintf("  Draught threshold removed:  %d\n", stats_count$draught_threshold_removed))
    cat(sprintf("  MARAD lookup misses:        %d\n", stats_count$marad_lookup_misses))
    cat(sprintf("  GMM failures:               %d\n", stats_count$gmm_failures))
    cat(sprintf("  GMM null returns:           %d\n", stats_count$gmm_null_returns))
    cat(sprintf("  Berthing events detected:   %d\n", stats_count$berthing_events_detected))
    cat(sprintf("  Traj. points filter removed:%d\n", stats_count$traj_points_filter_removed))
    cat(sprintf("  File load errors:           %d\n", stats_total$load_errors))
    cat(sprintf("  Pipeline errors:            %d\n", stats_total$pipeline_errors))

    cat("------------------------------------------------------------------------------\n\n")
    
    # Reset statistics
    stats_count[names(stats_count)] <- lapply(stats_count, function(x) 0)
    
    # Clean
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





