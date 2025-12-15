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






