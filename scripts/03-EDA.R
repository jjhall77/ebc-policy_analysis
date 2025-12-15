#=============================================================================
# EBC TARGETING ANALYSIS: Comparing Block Selection Strategies
# Purpose: Inform policymaker decisions on block-level intervention targeting
#=============================================================================

library(tidyverse)
library(sf)
library(here)

#-----------------------------------------------------------------------------
# PART 0: SETUP - Time window and intersection infrastructure
#-----------------------------------------------------------------------------
# Helper function for concentration curves
compute_concentration <- function(counts, option_name) {
  tibble(count = counts) |>
    filter(count > 0) |>
    arrange(desc(count)) |>
    mutate(
      cum_count = cumsum(count),
      total = sum(count),
      pct_crime = cum_count / total * 100,
      pct_blocks = row_number() / n() * 100,
      option = option_name
    )
}



# Analysis window: 5 years prior to 9/30/2025
end_date   <- as.Date("2025-09-30")
start_date <- as.Date("2020-10-01")

cat("Analysis window:", as.character(start_date), "to", as.character(end_date), "\n\n")

# For "past year" gun violence distribution
end_date_1yr   <- end_date
start_date_1yr <- end_date - years(1) + days(1)



#-----------------------------------------------------------------------------
# Build intersection infrastructure (from previous work)
#-----------------------------------------------------------------------------

# Load LION nodes
lion_gdb <- here("data", "lion", "lion.gdb")
nodes <- st_read(lion_gdb, layer = "node", quiet = TRUE) |>
  st_transform(2263) %>%
  clean_names()

# Filter LION to city streets
lion_streets <- lion |>
  filter(
    feature_typ %in% c("0", "6"),
    status == "2",
    segment_typ == "U",
    traf_dir %in% c("T", "A", "W"),
    !rw_type %in% c("2", "3", "4", "9")
  )

# Count street names per node to identify intersections
node_street_counts <- lion_streets |>
  st_drop_geometry() |>
  select(node_id_from, node_id_to, street) |>
  pivot_longer(cols = c(node_id_from, node_id_to), values_to = "nodeid") |>
  filter(!is.na(nodeid)) |>
  distinct(nodeid, street) |>
  count(nodeid, name = "n_streets") %>%
  mutate(nodeid = as.integer(nodeid))

# Intersections = nodes with 2+ street names
intersection_nodes <- nodes |>
  inner_join(node_street_counts |> filter(n_streets >= 2), by = "nodeid")

# Create 25ft intersection buffers
intersection_buffers <- intersection_nodes |>
  st_buffer(dist = 25) |>
  select(nodeid, n_streets)

cat("Intersection nodes identified:", nrow(intersection_nodes), "\n")

# Build intersection → adjacent physical_ids lookup
intersection_to_physical <- lion_streets |>
  st_drop_geometry() |>
  select(node_id_from, node_id_to, physical_id) |>
  filter(!is.na(physical_id)) |>
  pivot_longer(
    cols = c(node_id_from, node_id_to),
    names_to = "node_type",
    values_to = "nodeid"
  ) |>
  filter(!is.na(nodeid)) |>
  mutate(nodeid = as.integer(nodeid)) |>
  inner_join(
    intersection_nodes |> st_drop_geometry() |> select(nodeid),
    by = "nodeid"
  ) |>
  distinct(nodeid, physical_id)

# Count adjacent blocks per intersection (for equal split weights)
blocks_per_intersection <- intersection_to_physical |>
  count(nodeid, name = "n_adjacent")

cat("Intersection-to-physical mapping built\n\n")

#-----------------------------------------------------------------------------
# PART 1: PREPARE INCIDENT DATASETS
#-----------------------------------------------------------------------------

# Filter to analysis window
shootings_window <- shootings_sf |>
  mutate(date = mdy(occur_date)) |>
  filter(date >= start_date, date <= end_date)

shots_fired_window <- shots_fired |>
  filter(date >= start_date, date <= end_date)

violent_crime_window <- violent_crime |>
  filter(date >= start_date, date <= end_date)

violent_street_crime_window <- violent_street_crime |>
  filter(date >= start_date, date <= end_date)

cat("=== Incident Counts in Analysis Window ===\n")
cat("Shootings:", nrow(shootings_window), "\n")
cat("Shots fired:", nrow(shots_fired_window), "\n")
cat("Violent crime:", nrow(violent_crime_window), "\n")
cat("Violent street crime:", nrow(violent_street_crime_window), "\n\n")

#-----------------------------------------------------------------------------
# PART 2: FUNCTION - Assign incidents to blocks with equal intersection split
#-----------------------------------------------------------------------------

assign_to_blocks_equal_split <- function(incidents_sf, 
                                         physical_blocks,
                                         intersection_buffers,
                                         intersection_to_physical,
                                         blocks_per_intersection) {
  
  incidents_sf <- incidents_sf |>
    mutate(row_id = row_number())
  
  # Identify incidents at intersections
  at_intersection <- st_join(
    incidents_sf |> select(row_id),
    intersection_buffers,
    join = st_within
  ) |>
    st_drop_geometry() |>
    filter(!is.na(nodeid))
  
  intersection_row_ids <- unique(at_intersection$row_id)
  
  # Block-face incidents: nearest block, weight = 1
  block_face_incidents <- incidents_sf |>
    filter(!row_id %in% intersection_row_ids)
  
  if (nrow(block_face_incidents) > 0) {
    nearest_idx <- st_nearest_feature(block_face_incidents, physical_blocks)
    
    block_face_counts <- block_face_incidents |>
      st_drop_geometry() |>
      mutate(physical_id = physical_blocks$physical_id[nearest_idx]) |>
      group_by(physical_id) |>
      summarise(count = n(), .groups = "drop")
  } else {
    block_face_counts <- tibble(physical_id = integer(), count = numeric())
  }
  
  # Intersection incidents: equal split among adjacent blocks
  if (length(intersection_row_ids) > 0) {
    intersection_counts <- at_intersection |>
      distinct(row_id, nodeid) |>
      left_join(blocks_per_intersection, by = "nodeid") |>
      left_join(intersection_to_physical, by = "nodeid") |>
      mutate(weight = 1 / n_adjacent) |>
      group_by(physical_id) |>
      summarise(count = sum(weight), .groups = "drop")
  } else {
    intersection_counts <- tibble(physical_id = integer(), count = numeric())
  }
  
  # Combine
  all_counts <- bind_rows(block_face_counts, intersection_counts) |>
    group_by(physical_id) |>
    summarise(count = sum(count), .groups = "drop") |>
    arrange(desc(count)) |>
    mutate(rank = row_number())
  
  # Stats
  n_intersection <- length(intersection_row_ids)
  n_block_face <- nrow(incidents_sf) - n_intersection
  
  cat("  At intersections:", n_intersection, 
      "(", round(n_intersection / nrow(incidents_sf) * 100, 1), "%)\n")
  cat("  On block faces:", n_block_face, 
      "(", round(n_block_face / nrow(incidents_sf) * 100, 1), "%)\n")
  
  return(all_counts)
}

#-----------------------------------------------------------------------------
# PART 3: CREATE THE THREE TARGETING OPTIONS
#-----------------------------------------------------------------------------

cat("=== Building Targeting Option 1: Gun Violence ===\n")

# Combine shootings + shots fired
gun_violence_sf <- bind_rows(
  shootings_window |> 
    transmute(incident_type = "shooting", geometry),
  shots_fired_window |> 
    transmute(incident_type = "shots_fired", geometry)
)

cat("Total gun violence incidents:", nrow(gun_violence_sf), "\n")

gun_violence_blocks <- assign_to_blocks_equal_split(
  gun_violence_sf, physical_blocks, intersection_buffers,
  intersection_to_physical, blocks_per_intersection
) |>
  rename(gun_count = count, gun_rank = rank)

cat("\n=== Building Targeting Option 2: Violent Street Crime ===\n")
cat("Total violent street crime:", nrow(violent_street_crime_window), "\n")

street_crime_blocks <- assign_to_blocks_equal_split(
  violent_street_crime_window, physical_blocks, intersection_buffers,
  intersection_to_physical, blocks_per_intersection
) |>
  rename(street_count = count, street_rank = rank)

cat("\n=== Building Targeting Option 3: Violent Crime (All) ===\n")
cat("Total violent crime:", nrow(violent_crime_window), "\n")

violent_crime_blocks <- assign_to_blocks_equal_split(
  violent_crime_window, physical_blocks, intersection_buffers,
  intersection_to_physical, blocks_per_intersection
) |>
  rename(violent_count = count, violent_rank = rank)

#-----------------------------------------------------------------------------
# PART 4: MASTER BLOCK TABLE WITH ALL OPTIONS
#-----------------------------------------------------------------------------

# Start with all physical blocks
master_targeting <- physical_blocks |>
  st_drop_geometry() |>
  select(physical_id, boro, cd, ct2020) |>
  left_join(gun_violence_blocks, by = "physical_id") |>
  left_join(street_crime_blocks, by = "physical_id") |>
  left_join(violent_crime_blocks, by = "physical_id") |>
  mutate(
    across(c(gun_count, street_count, violent_count), ~replace_na(.x, 0)),
    # Recompute ranks including zeros
    gun_rank = rank(-gun_count, ties.method = "first"),
    street_rank = rank(-street_count, ties.method = "first"),
    violent_rank = rank(-violent_count, ties.method = "first")
  )

# Add precinct (from violent crime mode - most common precinct for crimes on block)
block_precinct <- violent_crime_window |>
  st_drop_geometry() |>
  mutate(physical_id = physical_blocks$physical_id[
    st_nearest_feature(violent_crime_window, physical_blocks)
  ]) |>
  count(physical_id, addr_pct_cd) |>
  group_by(physical_id) |>
  slice_max(n, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(physical_id, precinct = addr_pct_cd)

master_targeting <- master_targeting |>
  left_join(block_precinct, by = "physical_id")

# Add tier classification for each option
master_targeting <- master_targeting |>
  mutate(
    gun_tier = case_when(
      gun_rank <= 100 ~ "Top 100",
      gun_rank <= 200 ~ "101-200",
      TRUE ~ "Other"
    ),
    street_tier = case_when(
      street_rank <= 100 ~ "Top 100",
      street_rank <= 200 ~ "101-200",
      TRUE ~ "Other"
    ),
    violent_tier = case_when(
      violent_rank <= 100 ~ "Top 100",
      violent_rank <= 200 ~ "101-200",
      TRUE ~ "Other"
    )
  )

write_csv(master_targeting, here("output", "targeting_master_table.csv"))

#=============================================================================
# ANALYSIS 1: CONCENTRATION CURVES
#=============================================================================

cat("\n")
cat("================================================================\n")
cat("ANALYSIS 1: CRIME CONCENTRATION\n")
cat("================================================================\n\n")

compute_concentration <- function(counts, option_name) {
  df <- tibble(count = counts) |>
    filter(count > 0) |>
    arrange(desc(count)) |>
    mutate(
      cum_count = cumsum(count),
      total = sum(count),
      pct_crime = cum_count / total * 100,
      pct_blocks = row_number() / n() * 100,
      option = option_name
    )
  return(df)
}

gun_conc <- compute_concentration(master_targeting$gun_count, "Gun Violence")
street_conc <- compute_concentration(master_targeting$street_count, "Street Violent Crime")
violent_conc <- compute_concentration(master_targeting$violent_count, "All Violent Crime")

# Key thresholds
thresholds <- c(1, 2, 3, 5, 10, 20, 50)

concentration_summary <- map_dfr(thresholds, function(pct) {
  tibble(
    pct_blocks = pct,
    gun_violence = gun_conc$pct_crime[which.min(abs(gun_conc$pct_blocks - pct))],
    street_violent = street_conc$pct_crime[which.min(abs(street_conc$pct_blocks - pct))],
    all_violent = violent_conc$pct_crime[which.min(abs(violent_conc$pct_blocks - pct))]
  )
})

cat("% of crime captured by top X% of blocks:\n\n")
print(concentration_summary |> mutate(across(where(is.numeric), ~round(.x, 1))))

# Plot
concentration_all <- bind_rows(gun_conc, street_conc, violent_conc)

p_concentration <- ggplot(concentration_all, aes(x = pct_blocks, y = pct_crime, color = option)) +
  geom_line(linewidth = 1.2) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray50") +
  scale_color_brewer(palette = "Set1") +
  labs(
    title = "Crime Concentration by Targeting Option",
    subtitle = paste0("Analysis period: ", start_date, " to ", end_date),
    x = "% of Blocks (ranked by incidents)",
    y = "% of Incidents Captured",
    color = "Option"
  ) +
  theme_minimal() +
  coord_fixed(xlim = c(0, 50), ylim = c(0, 100))

p_concentration

ggsave(here("output", "concentration_curves.png"), p_concentration, width = 10, height = 8)

#=============================================================================
# ANALYSIS 2: BOROUGH BREAKDOWN
#=============================================================================

cat("\n")
cat("================================================================\n")
cat("ANALYSIS 2: BOROUGH BREAKDOWN OF TOP BLOCKS\n")
cat("================================================================\n\n")

boro_labels <- c("1" = "Manhattan", "2" = "Bronx", "3" = "Brooklyn", 
                 "4" = "Queens", "5" = "Staten Island")

borough_breakdown <- master_targeting |>
  mutate(boro_name = boro_labels[as.character(boro)]) |>
  group_by(boro_name) |>
  summarise(
    total_blocks = n(),
    # Gun violence
    gun_top100 = sum(gun_tier == "Top 100"),
    gun_101_200 = sum(gun_tier == "101-200"),
    # Street violent
    street_top100 = sum(street_tier == "Top 100"),
    street_101_200 = sum(street_tier == "101-200"),
    # All violent
    violent_top100 = sum(violent_tier == "Top 100"),
    violent_101_200 = sum(violent_tier == "101-200"),
    .groups = "drop"
  ) |>
  mutate(
    # Percentages
    gun_top100_pct = round(gun_top100 / sum(gun_top100) * 100, 1),
    gun_101_200_pct = round(gun_101_200 / sum(gun_101_200) * 100, 1),
    street_top100_pct = round(street_top100 / sum(street_top100) * 100, 1),
    street_101_200_pct = round(street_101_200 / sum(street_101_200) * 100, 1),
    violent_top100_pct = round(violent_top100 / sum(violent_top100) * 100, 1),
    violent_101_200_pct = round(violent_101_200 / sum(violent_101_200) * 100, 1)
  )

cat("Top 100 blocks by borough (counts and %):\n\n")
borough_breakdown |>
  select(boro_name, gun_top100, gun_top100_pct, street_top100, street_top100_pct,
         violent_top100, violent_top100_pct) |>
  print()

cat("\nBlocks 101-200 by borough:\n\n")
borough_breakdown |>
  select(boro_name, gun_101_200, gun_101_200_pct, street_101_200, street_101_200_pct,
         violent_101_200, violent_101_200_pct) |>
  print()

write_csv(borough_breakdown, here("output", "borough_breakdown.csv"))

#=============================================================================
# ANALYSIS 3: PRECINCT BREAKDOWN
#=============================================================================

cat("\n")
cat("================================================================\n")
cat("ANALYSIS 3: PRECINCT BREAKDOWN OF TOP BLOCKS\n")
cat("================================================================\n\n")

precinct_breakdown <- master_targeting |>
  filter(!is.na(precinct)) |>
  group_by(precinct) |>
  summarise(
    total_blocks = n(),
    gun_top100 = sum(gun_tier == "Top 100"),
    gun_101_200 = sum(gun_tier == "101-200"),
    street_top100 = sum(street_tier == "Top 100"),
    street_101_200 = sum(street_tier == "101-200"),
    violent_top100 = sum(violent_tier == "Top 100"),
    violent_101_200 = sum(violent_tier == "101-200"),
    .groups = "drop"
  ) |>
  arrange(desc(gun_top100))

cat("Top 15 precincts by gun violence top-100 blocks:\n\n")
precinct_breakdown |>
  head(15) |>
  print()

write_csv(precinct_breakdown, here("output", "precinct_breakdown.csv"))

# Visualization
precinct_long <- precinct_breakdown |>
  filter(gun_top100 > 0 | street_top100 > 0 | violent_top100 > 0) |>
  pivot_longer(
    cols = c(gun_top100, street_top100, violent_top100),
    names_to = "option",
    values_to = "n_blocks"
  ) |>
  mutate(option = case_when(
    option == "gun_top100" ~ "Gun Violence",
    option == "street_top100" ~ "Street Violent",
    option == "violent_top100" ~ "All Violent"
  ))

p_precinct <- ggplot(precinct_long |> filter(n_blocks > 0), 
                     aes(x = reorder(factor(precinct), -n_blocks), 
                         y = n_blocks, fill = option)) +
  geom_col(position = "dodge") +
  scale_fill_brewer(palette = "Set1") +
  labs(
    title = "Top 100 Blocks by Precinct and Targeting Option",
    x = "Precinct",
    y = "Number of Top-100 Blocks",
    fill = "Option"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1, size = 7))

p_precinct

ggsave(here("output", "precinct_breakdown.png"), p_precinct, width = 14, height = 6)

#=============================================================================
# ANALYSIS 4: NEIGHBORHOOD (NTA) BREAKDOWN
#=============================================================================

cat("\n")
cat("================================================================\n")
cat("ANALYSIS 4: NEIGHBORHOOD BREAKDOWN\n")
cat("================================================================\n\n")

# Load NTA boundaries
nta <- nynta

# Get NTA for each block centroid
block_centroids <- physical_blocks |>
  st_centroid()

block_nta <- st_join(block_centroids, nta |> select(nta_name)) |>
  st_drop_geometry() |>
  select(physical_id, nta = nta_name)

master_targeting <- master_targeting |>
  left_join(block_nta, by = "physical_id")

nta_breakdown <- master_targeting |>
  filter(!is.na(nta)) |>
  group_by(nta) |>
  summarise(
    total_blocks = n(),
    gun_top100 = sum(gun_tier == "Top 100"),
    gun_101_200 = sum(gun_tier == "101-200"),
    street_top100 = sum(street_tier == "Top 100"),
    street_101_200 = sum(street_tier == "101-200"),
    violent_top100 = sum(violent_tier == "Top 100"),
    violent_101_200 = sum(violent_tier == "101-200"),
    .groups = "drop"
  ) |>
  arrange(desc(gun_top100))

cat("Top 20 neighborhoods by gun violence top-100 blocks:\n\n")
nta_breakdown |>
  filter(gun_top100 > 0) |>
  head(20) |>
  print()

write_csv(nta_breakdown, here("output", "nta_breakdown.csv"))

#=============================================================================
# ANALYSIS 5: OVERLAP BETWEEN OPTIONS
#=============================================================================

cat("\n")
cat("================================================================\n")
cat("ANALYSIS 5: OVERLAP BETWEEN TARGETING OPTIONS\n")
cat("================================================================\n\n")

# Top 100 overlaps
gun_top100 <- master_targeting |> filter(gun_tier == "Top 100") |> pull(physical_id)
street_top100 <- master_targeting |> filter(street_tier == "Top 100") |> pull(physical_id)
violent_top100 <- master_targeting |> filter(violent_tier == "Top 100") |> pull(physical_id)

overlap_matrix_100 <- tibble(
  comparison = c("Gun ∩ Street", "Gun ∩ Violent", "Street ∩ Violent", "All Three"),
  n_overlap = c(
    length(intersect(gun_top100, street_top100)),
    length(intersect(gun_top100, violent_top100)),
    length(intersect(street_top100, violent_top100)),
    length(Reduce(intersect, list(gun_top100, street_top100, violent_top100)))
  ),
  pct_of_100 = round(n_overlap / 100 * 100, 1)
)

cat("TOP 100 OVERLAP:\n")
print(overlap_matrix_100)

# Top 101-200 overlaps
gun_101_200 <- master_targeting |> filter(gun_tier == "101-200") |> pull(physical_id)
street_101_200 <- master_targeting |> filter(street_tier == "101-200") |> pull(physical_id)
violent_101_200 <- master_targeting |> filter(violent_tier == "101-200") |> pull(physical_id)

overlap_matrix_200 <- tibble(
  comparison = c("Gun ∩ Street", "Gun ∩ Violent", "Street ∩ Violent", "All Three"),
  n_overlap = c(
    length(intersect(gun_101_200, street_101_200)),
    length(intersect(gun_101_200, violent_101_200)),
    length(intersect(street_101_200, violent_101_200)),
    length(Reduce(intersect, list(gun_101_200, street_101_200, violent_101_200)))
  ),
  pct_of_100 = round(n_overlap / 100 * 100, 1)
)

cat("\nBLOCKS 101-200 OVERLAP:\n")
print(overlap_matrix_200)

# Cross-tier overlap (e.g., gun top 100 vs street 101-200)
cat("\nCROSS-TIER OVERLAP:\n")
cat("Gun Top 100 in Street Top 100:", length(intersect(gun_top100, street_top100)), "\n")
cat("Gun Top 100 in Street 101-200:", length(intersect(gun_top100, street_101_200)), "\n")
cat("Gun Top 100 in Street Top 200 (combined):", 
    length(intersect(gun_top100, c(street_top100, street_101_200))), "\n")

# Save overlap analysis
overlap_summary <- bind_rows(
  overlap_matrix_100 |> mutate(tier = "Top 100"),
  overlap_matrix_200 |> mutate(tier = "101-200")
)
write_csv(overlap_summary, here("output", "overlap_analysis.csv"))

#=============================================================================
# ANALYSIS 6: CRIME CAPTURE RATES
#=============================================================================

cat("\n")
cat("================================================================\n")
cat("ANALYSIS 6: CRIME CAPTURE RATES BY TARGETING OPTION\n")
cat("================================================================\n\n")

# Total incidents
total_gun <- sum(master_targeting$gun_count)
total_street <- sum(master_targeting$street_count)
total_violent <- sum(master_targeting$violent_count)

# Calculate capture rates for each option × each crime type
capture_rates <- tibble(
  targeting_option = rep(c("Gun Violence", "Street Violent", "All Violent"), each = 2),
  tier = rep(c("Top 100", "101-200"), 3),
  
  # Capture of gun violence
  gun_captured = c(
    sum(master_targeting$gun_count[master_targeting$gun_tier == "Top 100"]),
    sum(master_targeting$gun_count[master_targeting$gun_tier == "101-200"]),
    sum(master_targeting$gun_count[master_targeting$street_tier == "Top 100"]),
    sum(master_targeting$gun_count[master_targeting$street_tier == "101-200"]),
    sum(master_targeting$gun_count[master_targeting$violent_tier == "Top 100"]),
    sum(master_targeting$gun_count[master_targeting$violent_tier == "101-200"])
  ),
  
  # Capture of street violent crime
  street_captured = c(
    sum(master_targeting$street_count[master_targeting$gun_tier == "Top 100"]),
    sum(master_targeting$street_count[master_targeting$gun_tier == "101-200"]),
    sum(master_targeting$street_count[master_targeting$street_tier == "Top 100"]),
    sum(master_targeting$street_count[master_targeting$street_tier == "101-200"]),
    sum(master_targeting$street_count[master_targeting$violent_tier == "Top 100"]),
    sum(master_targeting$street_count[master_targeting$violent_tier == "101-200"])
  ),
  
  # Capture of all violent crime
  violent_captured = c(
    sum(master_targeting$violent_count[master_targeting$gun_tier == "Top 100"]),
    sum(master_targeting$violent_count[master_targeting$gun_tier == "101-200"]),
    sum(master_targeting$violent_count[master_targeting$street_tier == "Top 100"]),
    sum(master_targeting$violent_count[master_targeting$street_tier == "101-200"]),
    sum(master_targeting$violent_count[master_targeting$violent_tier == "Top 100"]),
    sum(master_targeting$violent_count[master_targeting$violent_tier == "101-200"])
  )
) |>
  mutate(
    gun_pct = round(gun_captured / total_gun * 100, 1),
    street_pct = round(street_captured / total_street * 100, 1),
    violent_pct = round(violent_captured / total_violent * 100, 1)
  )

cat("Crime Capture Rates (% of each crime type captured):\n\n")
print(capture_rates)

write_csv(capture_rates, here("output", "capture_rates.csv"))

# Visualization
capture_long <- capture_rates |>
  select(targeting_option, tier, ends_with("_pct")) |>
  pivot_longer(cols = ends_with("_pct"), names_to = "crime_type", values_to = "pct") |>
  mutate(
    crime_type = case_when(
      crime_type == "gun_pct" ~ "Gun Violence",
      crime_type == "street_pct" ~ "Street Violent",
      crime_type == "violent_pct" ~ "All Violent"
    ),
    label = paste0(targeting_option, "\n", tier)
  )

p_capture <- ggplot(capture_long |> filter(tier == "Top 100"), 
                    aes(x = targeting_option, y = pct, fill = crime_type)) +
  geom_col(position = "dodge") +
  scale_fill_brewer(palette = "Set1") +
  labs(
    title = "Crime Capture Rates: Top 100 Blocks by Targeting Option",
    subtitle = "What % of each crime type is captured by top 100 blocks under each strategy?",
    x = "Targeting Option (how blocks were ranked)",
    y = "% of Crime Captured",
    fill = "Crime Type"
  ) +
  theme_minimal() +
  geom_text(aes(label = paste0(pct, "%")), position = position_dodge(0.9), vjust = -0.5, size = 3)

p_capture

ggsave(here("output", "capture_rates.png"), p_capture, width = 10, height = 6)

#=============================================================================
# ANALYSIS 7: CRIME TYPE BREAKDOWN (ofns_desc)
#=============================================================================

cat("\n")
cat("================================================================\n")
cat("ANALYSIS 7: OFFENSE CATEGORY BREAKDOWN (ofns_desc)\n")
cat("================================================================\n\n")

# Assign crimes to blocks first
violent_with_blocks <- violent_crime_window |>
  mutate(physical_id = physical_blocks$physical_id[
    st_nearest_feature(violent_crime_window, physical_blocks)
  ])

street_with_blocks <- violent_street_crime_window |>
  mutate(physical_id = physical_blocks$physical_id[
    st_nearest_feature(violent_street_crime_window, physical_blocks)
  ])

# For each crime, determine which tier it falls in under each option
violent_with_tiers <- violent_with_blocks |>
  st_drop_geometry() |>
  left_join(
    master_targeting |> select(physical_id, gun_tier, street_tier, violent_tier),
    by = "physical_id"
  )

street_with_tiers <- street_with_blocks |>
  st_drop_geometry() |>
  left_join(
    master_targeting |> select(physical_id, gun_tier, street_tier, violent_tier),
    by = "physical_id"
  )

# Offense breakdown for violent crime
ofns_violent <- violent_with_tiers |>
  group_by(ofns_desc) |>
  summarise(
    total = n(),
    in_gun_top100 = sum(gun_tier == "Top 100", na.rm = TRUE),
    in_gun_101_200 = sum(gun_tier == "101-200", na.rm = TRUE),
    in_street_top100 = sum(street_tier == "Top 100", na.rm = TRUE),
    in_street_101_200 = sum(street_tier == "101-200", na.rm = TRUE),
    in_violent_top100 = sum(violent_tier == "Top 100", na.rm = TRUE),
    in_violent_101_200 = sum(violent_tier == "101-200", na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    gun_top100_pct = round(in_gun_top100 / total * 100, 1),
    gun_top200_pct = round((in_gun_top100 + in_gun_101_200) / total * 100, 1),
    street_top100_pct = round(in_street_top100 / total * 100, 1),
    street_top200_pct = round((in_street_top100 + in_street_101_200) / total * 100, 1),
    violent_top100_pct = round(in_violent_top100 / total * 100, 1),
    violent_top200_pct = round((in_violent_top100 + in_violent_101_200) / total * 100, 1)
  ) |>
  arrange(desc(total))

cat("VIOLENT CRIME - % captured by offense type:\n\n")
ofns_violent |>
  select(ofns_desc, total, gun_top100_pct, street_top100_pct, violent_top100_pct) |>
  print()

# Same for street crime
ofns_street <- street_with_tiers |>
  group_by(ofns_desc) |>
  summarise(
    total = n(),
    in_gun_top100 = sum(gun_tier == "Top 100", na.rm = TRUE),
    in_street_top100 = sum(street_tier == "Top 100", na.rm = TRUE),
    in_violent_top100 = sum(violent_tier == "Top 100", na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    gun_top100_pct = round(in_gun_top100 / total * 100, 1),
    street_top100_pct = round(in_street_top100 / total * 100, 1),
    violent_top100_pct = round(in_violent_top100 / total * 100, 1)
  ) |>
  arrange(desc(total))

cat("\nSTREET VIOLENT CRIME - % captured by offense type:\n\n")
print(ofns_street)

write_csv(ofns_violent, here("output", "offense_breakdown_violent.csv"))
write_csv(ofns_street, here("output", "offense_breakdown_street.csv"))

#=============================================================================
# ANALYSIS 8: PD_DESC (SUBTYPE) BREAKDOWN
#=============================================================================

cat("\n")
cat("================================================================\n")
cat("ANALYSIS 8: CRIME SUBTYPE BREAKDOWN (pd_desc)\n")
cat("================================================================\n\n")

pd_violent <- violent_with_tiers |>
  group_by(pd_desc) |>
  summarise(
    total = n(),
    in_gun_top100 = sum(gun_tier == "Top 100", na.rm = TRUE),
    in_street_top100 = sum(street_tier == "Top 100", na.rm = TRUE),
    in_violent_top100 = sum(violent_tier == "Top 100", na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    gun_top100_pct = round(in_gun_top100 / total * 100, 1),
    street_top100_pct = round(in_street_top100 / total * 100, 1),
    violent_top100_pct = round(in_violent_top100 / total * 100, 1)
  ) |>
  arrange(desc(total))

cat("Top 20 violent crime subtypes:\n\n")
pd_violent |> head(20) |> print()

pd_street <- street_with_tiers |>
  group_by(pd_desc) |>
  summarise(
    total = n(),
    in_gun_top100 = sum(gun_tier == "Top 100", na.rm = TRUE),
    in_street_top100 = sum(street_tier == "Top 100", na.rm = TRUE),
    in_violent_top100 = sum(violent_tier == "Top 100", na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    gun_top100_pct = round(in_gun_top100 / total * 100, 1),
    street_top100_pct = round(in_street_top100 / total * 100, 1),
    violent_top100_pct = round(in_violent_top100 / total * 100, 1)
  ) |>
  arrange(desc(total))

cat("\nTop 20 street violent crime subtypes:\n\n")
pd_street |> head(20) |> print()

write_csv(pd_violent, here("output", "pd_desc_breakdown_violent.csv"))
write_csv(pd_street, here("output", "pd_desc_breakdown_street.csv"))


#=============================================================================
# ANALYSIS 8A: CRIME DISTRIBUTION ON TOP BLOCKS
# Question: Of all crimes on top blocks, what is the composition?
#=============================================================================

cat("\n")
cat("================================================================\n")
cat("ANALYSIS 8A: CRIME COMPOSITION ON TOP BLOCKS\n")
cat("================================================================\n")
cat("(What types of crimes occur on blocks selected by each strategy?)\n\n")

#-----------------------------------------------------------------------------
# 8A.1: ofns_desc distribution on top blocks
#-----------------------------------------------------------------------------

# Function to get crime distribution on a set of blocks
get_crime_distribution <- function(crime_df, block_ids, group_var) {
  crime_df |>
    filter(physical_id %in% block_ids) |>
    count({{ group_var }}, name = "n") |>
    mutate(
      pct = round(n / sum(n) * 100, 1),
      cum_pct = round(cumsum(n) / sum(n) * 100, 1)
    ) |>
    arrange(desc(n))
}

# Get block IDs for each tier/option
gun_top100_ids <- master_targeting |> filter(gun_tier == "Top 100") |> pull(physical_id)
gun_101_200_ids <- master_targeting |> filter(gun_tier == "101-200") |> pull(physical_id)
street_top100_ids <- master_targeting |> filter(street_tier == "Top 100") |> pull(physical_id)
street_101_200_ids <- master_targeting |> filter(street_tier == "101-200") |> pull(physical_id)
violent_top100_ids <- master_targeting |> filter(violent_tier == "Top 100") |> pull(physical_id)
violent_101_200_ids <- master_targeting |> filter(violent_tier == "101-200") |> pull(physical_id)

# --- OFNS_DESC distributions ---

cat("=== OFFENSE CATEGORY (ofns_desc) DISTRIBUTION ===\n\n")

# Gun Violence Top 100 - what violent crimes occur there?
cat("GUN VIOLENCE TOP 100 BLOCKS - Violent Crime Composition:\n")
ofns_on_gun_top100 <- get_crime_distribution(violent_with_tiers, gun_top100_ids, ofns_desc) |>
  mutate(targeting = "Gun Violence", tier = "Top 100")
print(ofns_on_gun_top100)

cat("\nGUN VIOLENCE 101-200 BLOCKS - Violent Crime Composition:\n")
ofns_on_gun_101_200 <- get_crime_distribution(violent_with_tiers, gun_101_200_ids, ofns_desc) |>
  mutate(targeting = "Gun Violence", tier = "101-200")
print(ofns_on_gun_101_200)

cat("\n---\n")
cat("STREET VIOLENT TOP 100 BLOCKS - Violent Crime Composition:\n")
ofns_on_street_top100 <- get_crime_distribution(violent_with_tiers, street_top100_ids, ofns_desc) |>
  mutate(targeting = "Street Violent", tier = "Top 100")
print(ofns_on_street_top100)

cat("\nSTREET VIOLENT 101-200 BLOCKS - Violent Crime Composition:\n")
ofns_on_street_101_200 <- get_crime_distribution(violent_with_tiers, street_101_200_ids, ofns_desc) |>
  mutate(targeting = "Street Violent", tier = "101-200")
print(ofns_on_street_101_200)

cat("\n---\n")
cat("ALL VIOLENT TOP 100 BLOCKS - Violent Crime Composition:\n")
ofns_on_violent_top100 <- get_crime_distribution(violent_with_tiers, violent_top100_ids, ofns_desc) |>
  mutate(targeting = "All Violent", tier = "Top 100")
print(ofns_on_violent_top100)

cat("\nALL VIOLENT 101-200 BLOCKS - Violent Crime Composition:\n")
ofns_on_violent_101_200 <- get_crime_distribution(violent_with_tiers, violent_101_200_ids, ofns_desc) |>
  mutate(targeting = "All Violent", tier = "101-200")
print(ofns_on_violent_101_200)

# Combine all ofns distributions
ofns_distribution_all <- bind_rows(
  ofns_on_gun_top100,
  ofns_on_gun_101_200,
  ofns_on_street_top100,
  ofns_on_street_101_200,
  ofns_on_violent_top100,
  ofns_on_violent_101_200
)

write_csv(ofns_distribution_all, here("output", "ofns_distribution_on_top_blocks.csv"))

#-----------------------------------------------------------------------------
# 8A.2: Side-by-side comparison table for ofns_desc
#-----------------------------------------------------------------------------

cat("\n")
cat("=== SIDE-BY-SIDE COMPARISON: ofns_desc ===\n\n")

# Pivot to wide format for easy comparison
ofns_comparison <- ofns_distribution_all |>
  mutate(label = paste0(targeting, "_", tier)) |>
  select(ofns_desc, label, pct) |>
  pivot_wider(names_from = label, values_from = pct, values_fill = 0) |>
  arrange(desc(`Gun Violence_Top 100`))

cat("% of violent crime by offense type on each set of blocks:\n\n")
print(ofns_comparison, n = 20)

write_csv(ofns_comparison, here("output", "ofns_comparison_wide.csv"))

#-----------------------------------------------------------------------------
# 8A.3: pd_desc distribution on top blocks
#-----------------------------------------------------------------------------

cat("\n")
cat("=== CRIME SUBTYPE (pd_desc) DISTRIBUTION ===\n\n")

# Gun Violence blocks
cat("GUN VIOLENCE TOP 100 BLOCKS - Crime Subtype Composition:\n")
pd_on_gun_top100 <- get_crime_distribution(violent_with_tiers, gun_top100_ids, pd_desc) |>
  mutate(targeting = "Gun Violence", tier = "Top 100")
print(pd_on_gun_top100 |> head(15))

cat("\nGUN VIOLENCE 101-200 BLOCKS - Crime Subtype Composition:\n")
pd_on_gun_101_200 <- get_crime_distribution(violent_with_tiers, gun_101_200_ids, pd_desc) |>
  mutate(targeting = "Gun Violence", tier = "101-200")
print(pd_on_gun_101_200 |> head(15))

cat("\n---\n")
cat("STREET VIOLENT TOP 100 BLOCKS - Crime Subtype Composition:\n")
pd_on_street_top100 <- get_crime_distribution(violent_with_tiers, street_top100_ids, pd_desc) |>
  mutate(targeting = "Street Violent", tier = "Top 100")
print(pd_on_street_top100 |> head(15))

cat("\nSTREET VIOLENT 101-200 BLOCKS - Crime Subtype Composition:\n")
pd_on_street_101_200 <- get_crime_distribution(violent_with_tiers, street_101_200_ids, pd_desc) |>
  mutate(targeting = "Street Violent", tier = "101-200")
print(pd_on_street_101_200 |> head(15))

cat("\n---\n")
cat("ALL VIOLENT TOP 100 BLOCKS - Crime Subtype Composition:\n")
pd_on_violent_top100 <- get_crime_distribution(violent_with_tiers, violent_top100_ids, pd_desc) |>
  mutate(targeting = "All Violent", tier = "Top 100")
print(pd_on_violent_top100 |> head(15))

cat("\nALL VIOLENT 101-200 BLOCKS - Crime Subtype Composition:\n")
pd_on_violent_101_200 <- get_crime_distribution(violent_with_tiers, violent_101_200_ids, pd_desc) |>
  mutate(targeting = "All Violent", tier = "101-200")
print(pd_on_violent_101_200 |> head(15))

# Combine all pd_desc distributions
pd_distribution_all <- bind_rows(
  pd_on_gun_top100,
  pd_on_gun_101_200,
  pd_on_street_top100,
  pd_on_street_101_200,
  pd_on_violent_top100,
  pd_on_violent_101_200
)

write_csv(pd_distribution_all, here("output", "pd_distribution_on_top_blocks.csv"))

#-----------------------------------------------------------------------------
# 8A.4: Side-by-side comparison table for pd_desc (top 20)
#-----------------------------------------------------------------------------

cat("\n")
cat("=== SIDE-BY-SIDE COMPARISON: pd_desc (Top 20) ===\n\n")

# Get top 20 pd_desc across all targeting options
top_pd_desc <- pd_distribution_all |>
  group_by(pd_desc) |>
  summarise(total_n = sum(n), .groups = "drop") |>
  slice_max(total_n, n = 20) |>
  pull(pd_desc)

pd_comparison <- pd_distribution_all |>
  filter(pd_desc %in% top_pd_desc) |>
  mutate(label = paste0(targeting, "_", tier)) |>
  select(pd_desc, label, pct) |>
  pivot_wider(names_from = label, values_from = pct, values_fill = 0) |>
  arrange(desc(`Gun Violence_Top 100`))

cat("% of violent crime by subtype on each set of blocks (top 20 subtypes):\n\n")
print(pd_comparison, n = 20)

write_csv(pd_comparison, here("output", "pd_comparison_wide.csv"))

#-----------------------------------------------------------------------------
# 8A.5: Summary - Key differences between targeting options
#-----------------------------------------------------------------------------

cat("\n")
cat("=== KEY DIFFERENCES IN CRIME MIX ===\n\n")

# Calculate the difference in composition between gun-targeted and street-targeted blocks
mix_diff <- ofns_comparison |>
  mutate(
    gun_vs_street_top100 = `Gun Violence_Top 100` - `Street Violent_Top 100`,
    gun_vs_violent_top100 = `Gun Violence_Top 100` - `All Violent_Top 100`
  ) |>
  select(ofns_desc, `Gun Violence_Top 100`, `Street Violent_Top 100`, 
         `All Violent_Top 100`, gun_vs_street_top100, gun_vs_violent_top100) |>
  arrange(desc(abs(gun_vs_street_top100)))

cat("Offense mix differences (positive = more on gun blocks):\n\n")
print(mix_diff)

write_csv(mix_diff, here("output", "offense_mix_differences.csv"))

#-----------------------------------------------------------------------------
# 8A.6: Visualization - Crime composition by targeting option
#-----------------------------------------------------------------------------

# Stacked bar chart
ofns_plot_data <- ofns_distribution_all |>
  filter(tier == "Top 100") |>
  group_by(targeting) |>
  slice_max(n, n = 5) |>  # Top 5 offense types per option
  ungroup()

p_composition <- ggplot(ofns_plot_data, 
                        aes(x = targeting, y = pct, fill = reorder(ofns_desc, -pct))) +
  geom_col(position = "stack") +
  scale_fill_brewer(palette = "Set3") +
  labs(
    title = "Violent Crime Composition on Top 100 Blocks",
    subtitle = "What types of crime occur on blocks selected by each strategy?",
    x = "Targeting Option",
    y = "% of Violent Crime on Those Blocks",
    fill = "Offense Type"
  ) +
  theme_minimal() +
  theme(legend.position = "right")

p_composition

ggsave(here("output", "crime_composition_top100.png"), p_composition, width = 12, height = 7)

# Faceted comparison
p_composition_facet <- ofns_distribution_all |>
  filter(ofns_desc %in% c("ASSAULT 3 & RELATED OFFENSES", "FELONY ASSAULT", 
                          "ROBBERY", "MURDER & NON-NEGL. MANSLAUGHTER")) |>
  ggplot(aes(x = tier, y = pct, fill = targeting)) +
  geom_col(position = "dodge") +
  facet_wrap(~ofns_desc, scales = "free_y") +
  scale_fill_brewer(palette = "Set1") +
  labs(
    title = "Crime Mix by Targeting Option and Tier",
    x = "Block Tier",
    y = "% of Crimes on Those Blocks",
    fill = "Targeting Option"
  ) +
  theme_minimal()

p_composition_facet

ggsave(here("output", "crime_composition_by_offense.png"), p_composition_facet, width = 12, height = 8)

cat("\nAnalysis 8A complete. Output files:\n")
cat("  - ofns_distribution_on_top_blocks.csv\n")
cat("  - ofns_comparison_wide.csv\n")
cat("  - pd_distribution_on_top_blocks.csv\n")
cat("  - pd_comparison_wide.csv\n")
cat("  - offense_mix_differences.csv\n")
cat("  - crime_composition_top100.png\n")
cat("  - crime_composition_by_offense.png\n")

#=============================================================================
# ANALYSIS 9: GUN VIOLENCE FREQUENCY DISTRIBUTION
#=============================================================================

cat("\n")
cat("================================================================\n")
cat("ANALYSIS 9: GUN VIOLENCE FREQUENCY DISTRIBUTION\n")
cat("================================================================\n\n")

# Past 5 years
gun_freq_5yr <- master_targeting |>
  filter(gun_count > 0) |>
  mutate(count_bucket = case_when(
    gun_count >= 20 ~ "20+",
    gun_count >= 15 ~ "15-19",
    gun_count >= 10 ~ "10-14",
    gun_count >= 8 ~ "8-9",
    gun_count >= 6 ~ "6-7",
    gun_count >= 5 ~ "5",
    gun_count >= 4 ~ "4",
    gun_count >= 3 ~ "3",
    gun_count >= 2 ~ "2",
    TRUE ~ "1"
  )) |>
  count(count_bucket, name = "n_blocks") |>
  mutate(
    count_bucket = factor(count_bucket, levels = c("1", "2", "3", "4", "5", "6-7", "8-9", "10-14", "15-19", "20+")),
    cum_blocks = cumsum(n_blocks),
    pct_blocks = round(n_blocks / sum(n_blocks) * 100, 1),
    cum_pct = round(cum_blocks / sum(n_blocks) * 100, 1),
    period = "5 Years"
  ) |>
  arrange(count_bucket)

cat("5-YEAR GUN VIOLENCE DISTRIBUTION:\n")
cat("(Blocks with at least X incidents)\n\n")
print(gun_freq_5yr)

# Past 1 year - need to recalculate
gun_1yr <- bind_rows(
  shootings_sf |>
    mutate(date = mdy(occur_date)) |>
    filter(date >= start_date_1yr, date <= end_date_1yr) |>
    transmute(incident_type = "shooting", geometry),
  shots_fired |>
    filter(date >= start_date_1yr, date <= end_date_1yr) |>
    transmute(incident_type = "shots_fired", geometry)
)

cat("\n1-year gun violence incidents:", nrow(gun_1yr), "\n")

gun_1yr_blocks <- assign_to_blocks_equal_split(
  gun_1yr, physical_blocks, intersection_buffers,
  intersection_to_physical, blocks_per_intersection
)

gun_freq_1yr <- gun_1yr_blocks |>
  filter(count > 0) |>
  mutate(count_bucket = case_when(
    count >= 10 ~ "10+",
    count >= 8 ~ "8-9",
    count >= 6 ~ "6-7",
    count >= 5 ~ "5",
    count >= 4 ~ "4",
    count >= 3 ~ "3",
    count >= 2 ~ "2",
    TRUE ~ "1"
  )) |>
  count(count_bucket, name = "n_blocks") |>
  mutate(
    count_bucket = factor(count_bucket, levels = c("1", "2", "3", "4", "5", "6-7", "8-9", "10+")),
    cum_blocks = cumsum(n_blocks),
    pct_blocks = round(n_blocks / sum(n_blocks) * 100, 1),
    cum_pct = round(cum_blocks / sum(n_blocks) * 100, 1),
    period = "1 Year"
  ) |>
  arrange(count_bucket)

cat("\n1-YEAR GUN VIOLENCE DISTRIBUTION:\n\n")
print(gun_freq_1yr)

# Cutoff analysis
cat("\n=== CUTOFF ANALYSIS ===\n")
cat("\n5-YEAR CUTOFFS:\n")
for (cutoff in c(3, 4, 5, 6, 8, 10, 15)) {
  n_blocks <- sum(master_targeting$gun_count >= cutoff)
  pct_gun <- round(sum(master_targeting$gun_count[master_targeting$gun_count >= cutoff]) / 
                     total_gun * 100, 1)
  cat(sprintf("  >= %d incidents: %d blocks capturing %.1f%% of gun violence\n", 
              cutoff, n_blocks, pct_gun))
}

cat("\n1-YEAR CUTOFFS:\n")
for (cutoff in c(2, 3, 4, 5, 6)) {
  n_blocks <- sum(gun_1yr_blocks$count >= cutoff)
  total_1yr <- sum(gun_1yr_blocks$count)
  pct_gun <- round(sum(gun_1yr_blocks$count[gun_1yr_blocks$count >= cutoff]) / 
                     total_1yr * 100, 1)
  cat(sprintf("  >= %d incidents: %d blocks capturing %.1f%% of 1yr gun violence\n", 
              cutoff, n_blocks, pct_gun))
}

gun_freq_combined <- bind_rows(
  gun_freq_5yr |> mutate(count_bucket = as.character(count_bucket)),
  gun_freq_1yr |> mutate(count_bucket = as.character(count_bucket))
)
write_csv(gun_freq_combined, here("output", "gun_frequency_distribution.csv"))

#=============================================================================
# ANALYSIS 10: BLENDED TARGETING EXPLORATION
#=============================================================================

cat("\n")
cat("================================================================\n")
cat("ANALYSIS 10: BLENDED TARGETING EXPLORATION\n")
cat("================================================================\n\n")

# Create composite scores
master_targeting <- master_targeting |>
  mutate(
    # Normalize counts to 0-1 scale
    gun_norm = gun_count / max(gun_count),
    street_norm = street_count / max(street_count),
    violent_norm = violent_count / max(violent_count),
    
    # Various blends
    blend_gun70_street30 = 0.7 * gun_norm + 0.3 * street_norm,
    blend_gun50_street50 = 0.5 * gun_norm + 0.5 * street_norm,
    blend_gun30_street70 = 0.3 * gun_norm + 0.7 * street_norm,
    
    # Ranks for blends
    blend_70_30_rank = rank(-blend_gun70_street30, ties.method = "first"),
    blend_50_50_rank = rank(-blend_gun50_street50, ties.method = "first"),
    blend_30_70_rank = rank(-blend_gun30_street70, ties.method = "first")
  )

# Get block IDs for each blend
blend_70_30_top100 <- master_targeting |> filter(blend_70_30_rank <= 100) |> pull(physical_id)
blend_50_50_top100 <- master_targeting |> filter(blend_50_50_rank <= 100) |> pull(physical_id)
blend_30_70_top100 <- master_targeting |> filter(blend_30_70_rank <= 100) |> pull(physical_id)

# Compare blend capture rates - use list() not c()
blend_capture <- tibble(
  blend = c("Gun Only", "70/30", "50/50", "30/70", "Street Only"),
  top100_blocks = list(
    gun_top100,
    blend_70_30_top100,
    blend_50_50_top100,
    blend_30_70_top100,
    street_top100
  )
) |>
  mutate(
    gun_captured = map_dbl(top100_blocks, ~sum(master_targeting$gun_count[master_targeting$physical_id %in% .x])),
    street_captured = map_dbl(top100_blocks, ~sum(master_targeting$street_count[master_targeting$physical_id %in% .x])),
    violent_captured = map_dbl(top100_blocks, ~sum(master_targeting$violent_count[master_targeting$physical_id %in% .x])),
    gun_pct = round(gun_captured / total_gun * 100, 1),
    street_pct = round(street_captured / total_street * 100, 1),
    violent_pct = round(violent_captured / total_violent * 100, 1)
  ) |>
  select(blend, gun_pct, street_pct, violent_pct)

cat("BLENDED TARGETING - Top 100 Capture Rates:\n\n")
print(blend_capture)

write_csv(blend_capture, here("output", "blend_comparison.csv"))

# Also show overlap between blends
cat("\n\nOVERLAP BETWEEN BLENDS:\n")
cat("Gun Only ∩ 70/30:", length(intersect(gun_top100, blend_70_30_top100)), "\n")
cat("Gun Only ∩ 50/50:", length(intersect(gun_top100, blend_50_50_top100)), "\n")
cat("Gun Only ∩ Street Only:", length(intersect(gun_top100, street_top100)), "\n")
cat("70/30 ∩ 50/50:", length(intersect(blend_70_30_top100, blend_50_50_top100)), "\n")
cat("50/50 ∩ 30/70:", length(intersect(blend_50_50_top100, blend_30_70_top100)), "\n")
#=============================================================================
# FINAL SUMMARY
#=============================================================================

cat("\n")
cat("================================================================\n")
cat("ANALYSIS COMPLETE - OUTPUT FILES CREATED\n")
cat("================================================================\n\n")

output_files <- c(
  "targeting_master_table.csv      - Full block data with all rankings",
  "concentration_curves.png        - Crime concentration visualization",
  "borough_breakdown.csv           - Top blocks by borough",
  "precinct_breakdown.csv          - Top blocks by precinct",
  "precinct_breakdown.png          - Precinct visualization",
  "nta_breakdown.csv               - Top blocks by neighborhood",
  "overlap_analysis.csv            - Overlap between options",
  "capture_rates.csv               - Crime capture by option",
  "capture_rates.png               - Capture rate visualization",
  "offense_breakdown_violent.csv   - ofns_desc breakdown (violent)",
  "offense_breakdown_street.csv    - ofns_desc breakdown (street)",
  "pd_desc_breakdown_violent.csv   - pd_desc breakdown (violent)",
  "pd_desc_breakdown_street.csv    - pd_desc breakdown (street)",
  "gun_frequency_distribution.csv  - Blocks by gun incident count",
  "blend_comparison.csv            - Blended targeting comparison"
)

cat(paste(output_files, collapse = "\n"))
cat("\n")
