#00a-create_working_datasets

#-------------------------------------------------
# Definitions
#-------------------------------------------------
violent_ky  <- c(101, 104, 105, 106, 344)
property_ky <- c(107, 109, 110)

outside_loc_keywords <- c(
  "FRONT OF","OPPOSITE OF","OUTSIDE","REAR OF",
  "STREET","IN STREET","SIDEWALK"
)

outside_prem_keywords <- c(
  "PARK","STREET","PUBLIC PLACE","HIGHWAY",
  "BRIDGE","SIDEWALK","VACANT LOT",
  "PUBLIC HOUSING AREA","OUTSIDE"
)

outside_loc_pattern  <- str_c(outside_loc_keywords, collapse = "|")
outside_prem_pattern <- str_c(outside_prem_keywords, collapse = "|")

#-------------------------------------------------
# Base sf (EPSG:2263) + flags
#-------------------------------------------------
compl_base <- complaints %>%
  filter(!is.na(x_coord_cd), !is.na(y_coord_cd)) %>%
  st_as_sf(
    coords = c("x_coord_cd", "y_coord_cd"),
    crs    = 2263,
    remove = FALSE
  ) %>%
  mutate(
    date = mdy(rpt_dt),
    loc_of_occur_desc = str_to_upper(coalesce(loc_of_occur_desc, "")),
    prem_typ_desc     = str_to_upper(coalesce(prem_typ_desc, "")),
    is_outdoor =
      str_detect(loc_of_occur_desc, outside_loc_pattern) |
      str_detect(prem_typ_desc,     outside_prem_pattern)
  )
#-------------------------------------------------
# 4 objects
#-------------------------------------------------
violent_crime <- compl_base %>%
  filter(ky_cd %in% violent_ky)

property_crime <- compl_base %>%
  filter(ky_cd %in% property_ky)

violent_street_crime <- compl_base %>%
  filter(ky_cd %in% violent_ky, is_outdoor)

property_street_crime <- compl_base %>%
  filter(ky_cd %in% property_ky, is_outdoor)



nypd_precinct_locations_sf <- nypd_precinct_locations %>%
  filter(!is.na(longitude), !is.na(latitude)) %>%
  st_as_sf(
    coords = c("longitude", "latitude"),
    crs = 4326,
    remove = FALSE
  ) %>%
  st_transform(2263)

shootings_sf <- shootings %>%
  filter(!is.na(x_coord_cd), !is.na(y_coord_cd)) %>%
  st_as_sf(
    coords = c("x_coord_cd", "y_coord_cd"),
    crs = 2263,
    remove = FALSE
  )



#comvine the shots fired datasets

shots_fired_new <- shots_fired_new %>%
  mutate(date = mdy(rec_create_dt))
#-------------------------------------------------
# 0) DEFINE CUTOFF DATE
#-------------------------------------------------
# Decision: cmplnt_key does NOT reliably link records across sources.
# Strategy: enforce a clean temporal handoff between datasets.
# Rule: sf_since_2017 ends the day BEFORE shots_fired_new begins.

cutoff_date <- min(shots_fired_new$date, na.rm = TRUE)
# e.g., 2022-09-12 → sf_since_2017 kept through 2022-09-11

#-------------------------------------------------
# 1) STANDARDIZE + CONDENSE EACH SOURCE
#-------------------------------------------------
# Decision: reduce each dataset to a shared "hybrid" schema
# so bind_rows() works cleanly and semantics are aligned.

# ---- shots_fired_since_2017 --------------------------------
# Decision: keep ONLY records before cutoff_date
# Rationale: avoid duplicate events in the overlap window

sf_2017_std <- shots_fired_since_2017 %>%
  mutate(date = mdy(rpt_dt)) %>%
  filter(date < cutoff_date) %>%   # HARD STOP before new file begins
  transmute(
    source = "shots_fired_since_2017",   # provenance retained
    date   = as.Date(date),
    pct    = as.integer(pct),
    
    # Core classification fields
    rpt_classfctn_desc = rpt_classfctn_desc,
    pd_desc   = pd_desc,
    ofns_desc = ofns_desc,
    
    # Geometry (NYC native coordinates)
    x = as.numeric(x_coord_cd),
    y = as.numeric(y_coord_cd)
  )

# ---- shots_fired_new ---------------------------------------
# Decision: keep ONLY records from cutoff_date onward
# Rationale: this file supersedes the older schema going forward

sf_new_std <- shots_fired_new %>%
  filter(date >= cutoff_date) %>%
  transmute(
    source = "shots_fired_new",
    date   = as.Date(date),
    pct    = as.integer(cmplnt_pct_cd),
    
    # Classification is thinner in new file; fill missing fields explicitly
    rpt_classfctn_desc = rpt_classfctn_desc,
    pd_desc   = NA_character_,
    ofns_desc = NA_character_,
    
    # Geometry (same CRS as sf_since_2017)
    x = as.numeric(x_coordinate_code),
    y = as.numeric(y_coordinate_code)
  )

#-------------------------------------------------
# 2) CONVERT EACH TO sf (EPSG:2263) *BEFORE* MERGE
#-------------------------------------------------
# Decision: enforce consistent CRS prior to binding.
# Rationale: prevents silent geometry coercion and downstream bugs.

sf_2017_sf <- sf_2017_std %>%
  filter(!is.na(x), !is.na(y)) %>%     # Drop non-geocoded rows
  st_as_sf(
    coords = c("x", "y"),
    crs    = 2263,
    remove = FALSE
  )

sf_new_sf <- sf_new_std %>%
  filter(!is.na(x), !is.na(y)) %>%
  st_as_sf(
    coords = c("x", "y"),
    crs    = 2263,
    remove = FALSE
  )

#-------------------------------------------------
# 3) HYBRID sf DATASET (NO TEMPORAL OVERLAP)
#-------------------------------------------------
# Decision: simple bind_rows() is now safe
# because:
#   • schemas are identical
#   • CRS is identical
#   • time windows do not overlap

shots_fired <- bind_rows(
  sf_2017_sf,
  sf_new_sf
)

#-------------------------------------------------
# 4) SANITY CHECKS 
#-------------------------------------------------
range(shots_fired$date)      # confirm clean handoff
shots_fired %>% count(source)
st_crs(shots_fired)




glimpse(violent_crime)
#glimpse(property_crime)
glimpse(violent_street_crime)
#glimpse(property_street_crime)

glimpse(nypd_precinct_locations_sf)
glimpse(shootings_sf)
glimpse(shots_fired)
glimpse(physical_blocks)

