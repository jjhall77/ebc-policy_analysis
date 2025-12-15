#00-load_data
library(here)
library(tidyverse)
library(sf)
library(janitor)

list.files(here("data/lion"))

# Path to the geodatabase
lion_gdb <- here("data", "lion", "lion.gdb")

# List available layers inside the .gdb
st_layers(lion_gdb)

lion_gdb <- here("data", "lion", "lion.gdb")

lion <- st_read(lion_gdb, layer = "lion") %>%
  st_transform(2263) %>% 
  clean_names() # NYC coordinates, ft


#-------------------------------------------------
# NYPD Complaint data (CSV → bind_rows)
#-------------------------------------------------

complaints_current <- read_csv(
  "./data/NYPD_Complaint_Data_Current_(Year_To_Date)_20251214.csv",
  show_col_types = FALSE
) %>%
  clean_names() %>%
  mutate(housing_psa = as.character(housing_psa))

complaints_historic <- read_csv(
  "./data/NYPD_Complaint_Data_Historic_20251214.csv",
  show_col_types = FALSE
) %>%
  clean_names() %>%
  mutate(housing_psa = as.character(housing_psa))

complaints <- bind_rows(complaints_historic, complaints_current)

glimpse(complaints)



#-------------------------------------------------
# NYPD precinct locations (CSV)
#-------------------------------------------------

nypd_precinct_locations <- read_csv(
  "./data/nypd_precinct_locations.csv",
  show_col_types = FALSE
) %>%
  clean_names()

glimpse(nypd_precinct_locations)

#-------------------------------------------------
# NYPD precinct polygons (shapefile)
#-------------------------------------------------

nypp <- st_read(
  "./data/nypp_25d",
  quiet = TRUE
) %>%
  st_transform(2263) %>%   # NYC feet, consistent with LION, etc.
  clean_names()

nypp

#-------------------------------------------------
# NYC Neighborhood Tabulation Areas (NTA)
#-------------------------------------------------

nynta <- st_read(
  "./data/nynta2020_25d",
  quiet = TRUE
) %>%
  st_transform(2263) %>%
  clean_names()

nynta


#-------------------------------------------------
# NYPD Shooting Incident data (CSV → bind_rows)
#-------------------------------------------------

shootings_historic <- read_csv(
  "./data/NYPD_Shooting_Incident_Data_(Historic)_20251215.csv",
  show_col_types = FALSE
) %>%
  clean_names() %>%
  mutate(statistical_murder_flag = as.character(statistical_murder_flag))

shootings_current <- read_csv(
  "./data/NYPD_Shooting_Incident_Data_(Year_To_Date)_20251215.csv",
  show_col_types = FALSE
) %>%
  clean_names()

shootings <- bind_rows(
  shootings_historic,
  shootings_current
)

glimpse(shootings)

#-------------------------------------------------
# Shots fired data (CSV → load only, no bind)
#-------------------------------------------------

shots_fired_since_2017 <- read_csv(
  "./data/sf_since_2017.csv",
  show_col_types = FALSE
) %>%
  clean_names()

shots_fired_new <- read_csv(
  "./data/shots_fired_new.csv",
  show_col_types = FALSE
) %>%
  clean_names()

glimpse(shots_fired_since_2017)
glimpse(shots_fired_new)


glimpse(nypd_precinct_locations)
glimpse(shootings)
