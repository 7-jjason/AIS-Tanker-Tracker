# setwd("/Users/josephjason/Documents/Forecasting/R/projects/AIS Tanker Tracker")

# remotes::install_github("ropensci/tabulapdf")
library(tabulapdf)
library(tibble)
library(stringr)
library(sf)
library(tmap)
library(cols4all)

#############################################################################
# Port List Creation ------------------------------------------------------
#############################################################################

# Automatically detect and extract tables
pdf <- extract_tables("seaports_of_the_world.pdf",
                         method = "lattice",
                         output = "tibble")

pdf_list <- lapply(pdf, function(pages) {
  pages <- as_tibble(pages)
  pages <- pages[complete.cases(pages), ]
})

pdf_dt <- rbindlist(pdf_list, fill = TRUE)
pdf_dt <- pdf_dt[2:nrow(pdf_dt), ]
setnames(pdf_dt, c("Country_rm",
                   "Port_rm",
                   "CODE_rm",
                   "Lat/Long_rm",
                   "Telephone_rm",
                   "Web_rm",
                   "Country",
                   "Port",
                   "CODE",
                   "Lat/Long",
                   "Telephone",
                   "Web"))
pdf_dt <- pdf_dt[, `:=` (Country = fifelse(!is.na(Country), Country, Country_rm),
                         Port = fifelse(!is.na(Port), Port, Port_rm),
                         CODE = fifelse(!is.na(CODE), CODE, CODE_rm),
                         `Lat/Long` = fifelse(!is.na(`Lat/Long`), `Lat/Long`, `Lat/Long_rm`),
                         Telephone = fifelse(!is.na(Telephone), Telephone, Telephone_rm),
                         Web = fifelse(!is.na(Web), Web, Web_rm))]
pdf_dt[, `:=` (Country_rm = NULL,
               Port_rm = NULL,
               CODE_rm = NULL,
               `Lat/Long_rm` = NULL,
               Telephone_rm = NULL,
               Web_rm = NULL)]

dt_cl <- pdf_dt

# Example string from your PDF: "41° 19' N / 19° 28' E"
dt_cl[, c("lat_deg", "lat_min", "lat_dir", "lon_deg", "lon_min", "lon_dir") :=
        tstrsplit(`Lat/Long`, "[°' /]+", type.convert = TRUE)]

# Convert to Decimal
dt_cl[, lat_dec := lat_deg + (lat_min / 60)]
dt_cl[, lon_min := as.numeric(lon_min)]
dt_cl[, lon_deg := as.numeric(lon_deg)]
dt_cl[, lon_dec := lon_deg + (as.numeric(lon_min) / 60)]

# Adjust for South or West (if applicable)
dt_cl[lat_dir == "S", lat_dec := lat_dec * -1]
dt_cl[lon_dir == "W", lon_dec := lon_dec * -1]

dt_cl <- dt_cl[!is.na(lon_dec) & !is.na(lat_dec)]

# Convert your Port List to a Spatial Object
ports_sf <- st_as_sf(dt_cl, coords = c("lon_dec", "lat_dec"), crs = 4326)

#############################################################################
# Match New Data ----------------------------------------------------------
#############################################################################

# Convert your External Data to a Spatial Object
# (Assuming your external data has 'lat' and 'lon' columns)
external_dt <- readRDS("data/output/all_cargo_events.rds")
external_data_sf <- st_as_sf(external_dt, coords = c("end_lon", "end_lat"), crs = 4326)

# Find the index of the nearest port for every row in external_data
nearest_indices <- st_nearest_feature(external_data_sf, ports_sf)

# Pull the Port Metadata into your external data
external_dt[, matched_port := dt_cl$Port[nearest_indices]]
external_dt[, matched_code := dt_cl$CODE[nearest_indices]]
external_dt[, matched_lat := dt_cl$lat_dec[nearest_indices]]
external_dt[, matched_lon := dt_cl$lon_dec[nearest_indices]]

#############################################################################
# Find Summary Statistics -------------------------------------------------
#############################################################################

# Directional weight (loading/unloading) for total
external_dt[, directional_weight := fifelse(event_type == "loading",
                                            cargo_change_tonnes, # loading
                                            -cargo_change_tonnes)] # unloading
# Summarize
port_summary <- external_dt[, .(
  # Sum only the values where event_type matches
  total_loaded   = sum(cargo_change_tonnes[event_type == "loading"], na.rm = TRUE),
  total_unloaded = sum(cargo_change_tonnes[event_type == "unloading"], na.rm = TRUE),
  # Net is Loaded minus Unloaded
  total_net_tonnes = sum(directional_weight, na.rm = TRUE),
  # Metadata
  event_count = .N,
  lat = matched_lat[1], 
  lon = matched_lon[1]
), by = .(primary_cargo, matched_port, matched_code)]

setcolorder(port_summary, c("matched_port", 
                            "matched_code", 
                            "lat", 
                            "lon", 
                            "primary_cargo", 
                            "total_loaded", 
                            "total_unloaded", 
                            "total_net_tonnes",
                            "event_count"))

setnames(port_summary, 
         old = c("matched_port",
                 "matched_code"),
         new = c("port_name",
                 "port_code"))

# saveRDS(object = port_summary,
#         file = "data/port_list.rds")

#############################################################################
# Spatial Mapping ---------------------------------------------------------
#############################################################################

# Load the world shapefile
data("World") 
# Ensure the World map uses the same coordinate system (CRS 4326)
World <- st_transform(World, 4326)

# Convert summary to sf object and filter out NAs to prevent st_as_sf error
port_sf <- st_as_sf(port_summary[!is.na(lat) & !is.na(lon)], 
                    coords = c("lon", "lat"), crs = 4326)

# Perform the spatial join 
# 'st_join' checks which World polygon contains each port point
port_with_country <- st_join(port_sf, World["name"], join = st_intersects)

# Fix "offshore" ports (those that didn't intersect exactly)
# We find the nearest country index for the ones that came up NA
na_indices <- which(is.na(port_with_country$name))
if(length(na_indices) > 0){
  nearest <- st_nearest_feature(port_sf[na_indices, ], World)
  port_with_country$name[na_indices] <- World$name[nearest]
}

# Final table
port_summary_final <- as.data.table(port_with_country)
setnames(port_summary_final, "name", "country_name") 

saveRDS(object = port_summary_final, file = "data/port_list_2.rds")


# Aggregate port data to country level for the "Fill" color
country_summary <- port_summary_final[, .(
  country_load = sum(total_loaded, na.rm = TRUE),
  country_unload = sum(total_unloaded, na.rm = TRUE)
), by = country_name]

# Join this back to the World shapefile
world_mapped <- merge(World, country_summary, by.x = "name", by.y = "country_name", all.x = TRUE)

#############################################################################
# Plots -------------------------------------------------------------------
#############################################################################

# Loading 
# Map 1: Total Loading (Exports)
tm_shape(world_mapped) +
  tm_polygons(
    fill = "country_load", 
    fill.scale = tm_scale_intervals(
      # c4a to scale darker
      values = c4a("brewer.blues", n = 9)[3:9], 
      # type of break
      style = "fisher",
      value.na = "white"       
    ),
    fill.legend = tm_legend(title = "Total Loaded (Tonnes)") 
  ) +
  tm_title("Global Export Intensity by Country") 

# Unloading 
tm_shape(world_mapped) +
  tm_polygons(
    fill = "country_unload", 
    fill.scale = tm_scale_intervals(
      values = c4a("brewer.oranges", n = 9)[3:9], 
      style = "fisher",
      value.na = "white"
    ),
    fill.legend = tm_legend(title = "Total Unloaded (Tonnes)")
  ) +
  tm_title("Global Import Intensity by Country")

