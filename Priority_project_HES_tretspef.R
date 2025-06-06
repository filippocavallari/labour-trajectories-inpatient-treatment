setwd("/home/cdsw/")

run_sample_flow <- FALSE
run_checks <- FALSE

#--------------
# Load packages
#--------------

library(dplyr)
library(sparklyr)
library(ggplot2)
library(uuid)
library(dbplyr)
library(writexl)

#----------------------------
# Set up the spark connection
#----------------------------

set.seed(123)

spark_config <- spark_config()
spark_config$spark.executor.memory <- "58g"
#spark_config$spark.yarn.executor.memoryOverhead <- "6g"
spark_config$spark.executor.cores <- 5
spark_config$spark.dynamicAllocation.enabled <- "true"
#spark_config$spark.dynamicAllocation.maxExecutors <- 12
spark_config$spark.sql.shuffle.partitions <- 2000
#spark_config$spark.shuffle.service.enabled <- "true"
spark_config$spark.sql.parquet.int96RebaseModeInRead <- "LEGACY" #"CORRECTED"
spark_config$spark.sql.parquet.int96RebaseModeInWrite <- "LEGACY" #"CORRECTED"
spark_config$spark.sql.legacy.timeParserPolicy <- "LEGACY"
spark_config$spark.sql.session.timeZone <- "UTC+01:00" # "GMT"
spark_config$spark.network.timeout <- "600s"
spark_config$spark.driver.maxResultSize <- '10g'

sc <- spark_connect(master = "yarn-client",
                    app_name = "hmrc_outcome_TD",
                    config = spark_config)

#----------------
# Run config file
#----------------

#---------------------------#
# Initialise sample flow 
#---------------------------#

# Initialize the sample flow to store counts and descriptions
sample_flow <- data.frame(stage = character(),
                          count = numeric(0))

#------------
# Census 2011
#------------

sdf_census2011 <- sdf_sql(sc, paste0("SELECT
                                      IL4_PERSON_ID, URESINDPUK11, SEX, ETHPUK11, DISABILITY,
                                      NSSEC, NSSHUK11, RELPUK11, HLQPUK11, HEALTH, DEPRIVED,
                                      REGION_CODE, RURALURBAN_CODE, DOB_QUARTER, CEN_PR_FLAG,
                                      COUNTRY_CODE, PRESENT_IN_CENMORTLINK 
                                      FROM ", table_census2011))

sdf_census2011 <- recode_census2011_variables(sdf_census2011)

# Update sample flow
if (run_sample_flow == TRUE) {     
  # Update sample flow 
  sample_flow <- add_row(sample_flow,
                         stage = "census2011",
                         count = sdf_nrow(sdf_census2011))
  print(sample_flow)
}

#-------
# Deaths
#-------

sdf_deaths_2009 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2009))
sdf_deaths_2010 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2010))
sdf_deaths_2011 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2011))
sdf_deaths_2012 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2012))
sdf_deaths_2013 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2013))
sdf_deaths_2014 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2014))
sdf_deaths_2015 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2015))
sdf_deaths_2016 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2016))
sdf_deaths_2017 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2017))
sdf_deaths_2018 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2018))
sdf_deaths_2019 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2019))
sdf_deaths_2020 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2020))
sdf_deaths_2021 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2021))
sdf_deaths_2022 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2022))
sdf_deaths_2023 <- sdf_sql(sc, paste0("SELECT CENSUS2011_PERSON_ID, DODDY, DODMT, DODYR, DOR FROM ", table_deaths_2023))

sdf_deaths <- sdf_bind_rows(sdf_deaths_2009, sdf_deaths_2010, sdf_deaths_2011, sdf_deaths_2012, sdf_deaths_2013,
                            sdf_deaths_2014, sdf_deaths_2015, sdf_deaths_2016, sdf_deaths_2017, sdf_deaths_2018,
                            sdf_deaths_2019, sdf_deaths_2020, sdf_deaths_2021, sdf_deaths_2022, sdf_deaths_2023)

# Update sample flow
if (run_sample_flow == TRUE) {     
  # Update sample flow 
  sample_flow <- add_row(sample_flow,
                         stage = "sdf_deaths_all",
                         count = sdf_nrow(sdf_deaths)) 
  print(sample_flow)
}

sdf_deaths <- sdf_deaths %>%
  rename_with(.cols = everything(), .fn = tolower) %>%
  filter(!is.na(census2011_person_id)) %>%  
  mutate(across(c(doddy, dodmt, dodyr), ~ as.character(as.integer(.x)))) %>%
  mutate(across(c(doddy, dodmt), ~ ifelse(nchar(.x) == 1, paste0("0", .x), .x))) %>%
  mutate(dod = paste0(dodyr, "-", dodmt, "-", doddy)) %>%
  mutate(dod = to_date(dod, "yyyy-MM-dd")) %>%
  mutate(dor = paste0(substr(dor, 1, 4), "-", substr(dor, 5, 6), "-", substr(dor, 7, 8))) %>%
  mutate(dor = to_date(dor, "yyyy-MM-dd")) %>%
  filter(!is.na(dod))

# Update sample flow
if (run_sample_flow == TRUE) {     
  # Update sample flow 
  sample_flow <- add_row(sample_flow,
                         stage = "sdf_deaths_with_valid_dod",
                         count = sdf_nrow(sdf_deaths)) 
  print(sample_flow)
}

sdf_deaths <- sdf_deaths %>%
  select(census2011_person_id, dod, dor) %>%
  group_by(census2011_person_id) %>%
  summarise(dod = min(dod, na.rm = TRUE),
            dor = min(dor, na.rm = TRUE)) %>%
  ungroup() %>%
  sdf_drop_duplicates()
  
# Update sample flow
if (run_sample_flow == TRUE) {     
  # Update sample flow 
  sample_flow <- add_row(sample_flow,
                         stage = "sdf_deaths_min_dod",
                         count = sdf_nrow(sdf_deaths)) 
  print(sample_flow)
}

#----
# HES
#----

df_lookup_icd10 <- read.csv(lookup_icd10, header = TRUE, stringsAsFactors = FALSE) %>%
  dplyr::mutate(diag_4_01 = ifelse(nchar(Code) == 3, paste0(Code, "X"), Code)) %>%
  dplyr::select(Name,Group, diag_4_01) %>%
  sparklyr::sdf_copy_to(sc, ., "df_lookup_icd10", overwrite = TRUE)

sdf_hes_ip_all <- load_hes_ip(df_lookup_icd10)

# Update sample flow
if (run_sample_flow == TRUE) {     
  # Update sample flow 
  sample_flow <- add_row(sample_flow,
                         stage = "hes_all_episodes",
                         count = sdf_nrow(sdf_hes_ip_all)) 
  print(sample_flow)
}

# Filter to those with a census ID
sdf_hes_ip <- filter(sdf_hes_ip_all, !is.na(census_person_id_2011))

# Update sample flow
if (run_sample_flow == TRUE) {     
  # Update sample flow 
  sample_flow <- add_row(sample_flow,
                         stage = "hes_with_census_id",
                         count = sdf_nrow(sdf_hes_ip))
  print(sample_flow)
}

# Has a treatment code
sdf_hes_ip <- filter(sdf_hes_ip, !is.na(tretspef) & tretspef != "&")

# Update sample flow
if (run_sample_flow == TRUE) {     
  # Update sample flow 
  sample_flow <- add_row(sample_flow,
                         stage = "hes_with_valid_treatment_code",
                         count = sdf_nrow(sdf_hes_ip))
  sample_flow <- add_row(sample_flow,
                         stage = "hes_with_valid_treatment_code_person_level",
                         count = sdf_nrow(distinct(sdf_hes_ip, census_person_id_2011)))
  print(sample_flow)
}

sdf_hes_ip %>% select(tretspef) %>% arrange(tretspef) %>% head()
sdf_hes_ip %>% select(tretspef) %>% arrange(desc(tretspef)) %>% head()

# Admin method = non emergency
sdf_hes_ip <- sdf_hes_ip %>% filter(admimeth %in% 11:13)

# Update sample flow
if (run_sample_flow == TRUE) {     
  # Update sample flow 
  sample_flow <- add_row(sample_flow,
                         stage = "hes_non_emergency_admission",
                         count = sdf_nrow(sdf_hes_ip))
   sample_flow <- add_row(sample_flow,
                         stage = "hes_non_emergency_admission_person_level",
                         count = sdf_nrow(distinct(sdf_hes_ip, census_person_id_2011)))
  print(sample_flow)
}

sdf_hes_ip %>% group_by(admimeth) %>% count() %>% ungroup() %>% arrange(admimeth) %>% collect() %>% data.frame()

# Has a valid elecdate
sdf_hes_ip <- sdf_hes_ip %>%
  mutate(elecdate = to_date(elecdate, "yyyy-MM-dd")) %>% 
  filter(!is.na(elecdate) & elecdate != "1800-01-01" & elecdate != "1801-01-01")

if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "hes_remove_NA_elecdate",
                         count = sdf_nrow(sdf_hes_ip))  
  sample_flow <- add_row(sample_flow,
                         stage = "hes_remove_NA_elecdate_person_level",
                         count = sdf_nrow(distinct(sdf_hes_ip, census_person_id_2011)))
  print(sample_flow)
}

sdf_hes_ip %>% select(elecdate) %>% arrange(elecdate) %>% head()
sdf_hes_ip %>% select(elecdate) %>% arrange(desc(elecdate)) %>% head()

# Has an elecdur waiting time
sdf_hes_ip <- sdf_hes_ip %>% filter(!is.na(elecdur))

if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "hes_remove_NA_elecdur",
                         count = sdf_nrow(sdf_hes_ip))  
  sample_flow <- add_row(sample_flow,
                         stage = "hes_remove_NA_elecdur_person_level",
                         count = sdf_nrow(distinct(sdf_hes_ip, census_person_id_2011)))
  print(sample_flow)
}

sdf_hes_ip %>% select(elecdur) %>% arrange(elecdur) %>% head()
sdf_hes_ip %>% select(elecdur) %>% arrange(desc(elecdur)) %>% head()

# Has an elecdur waiting time within 2 years
sdf_hes_ip <- sdf_hes_ip %>% filter(elecdur <= 730)

if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "hes_remove_wt_more_than_2_years",
                         count = sdf_nrow(sdf_hes_ip))  
  sample_flow <- add_row(sample_flow,
                         stage = "hes_remove_wt_more_than_2_years_person_level",
                         count = sdf_nrow(distinct(sdf_hes_ip, census_person_id_2011)))
  print(sample_flow)
}

sdf_hes_ip %>% select(elecdur) %>% arrange(elecdur) %>% head()
sdf_hes_ip %>% select(elecdur) %>% arrange(desc(elecdur)) %>% head()

# Admin start date in study period
sdf_hes_ip <- sdf_hes_ip %>%
  mutate(admidate = to_date(admidate, "yyyy-MM-dd")) %>% 
  filter(!is.na(admidate)) %>%
  filter(admidate >= "2015-04-01") %>%
  select(census2011_person_id = census_person_id_2011, 
         admidate, tretspef, elecdate, elecdur, admimeth,
         diag_4_01, Name, Group, opertn_4_01, procodet)

sdf_hes_ip %>% select(admidate) %>% arrange(admidate) %>% head()
sdf_hes_ip %>% select(admidate) %>% arrange(desc(admidate)) %>% head()

# Update sample flow
if (run_sample_flow == TRUE) {     
  # Update sample flow 
  sample_flow <- add_row(sample_flow,
                         stage = "hes_admidate_in_study_period",
                         count = sdf_nrow(sdf_hes_ip))
  sample_flow <- add_row(sample_flow,
                         stage = "hes_admidate_in_study_period_person_level",
                         count = sdf_nrow(distinct(sdf_hes_ip, census2011_person_id)))
  print(sample_flow)
}

# Filter to earliest date for each person and treatment code
earliest_records <- sdf_hes_ip %>%
  group_by(census2011_person_id, tretspef) %>%
  summarise(earliest_date = min(admidate, na.rm = TRUE), .groups = "drop") %>%
  ungroup() %>%
  sdf_drop_duplicates() %>%
  mutate(earliest_date_flag = 1)

sdf_hes_ip <- sdf_hes_ip %>%
  left_join(earliest_records, by = c("census2011_person_id", "tretspef", "admidate" = "earliest_date")) %>%
  filter(earliest_date_flag == 1) %>%
  sdf_drop_duplicates() %>%
  select(-earliest_date_flag)

# Update sample flow
if (run_sample_flow == TRUE) {     
  # Update sample flow 
  sample_flow <- add_row(sample_flow,
                         stage = "hes_earliest_date_per_treatment",
                         count = sdf_nrow(sdf_hes_ip))
  sample_flow <- add_row(sample_flow,
                         stage = "hes_earliest_date_per_treatment_person_level",
                         count = sdf_nrow(distinct(sdf_hes_ip, census2011_person_id)))
  print(sample_flow)
}

# Filter to those with max one row per treatment
#sdf_hes_ip <- sdf_hes_ip %>%
#  group_by(census2011_person_id, tretspef) %>%
#  mutate(num_rows = count()) %>%
#  ungroup() %>% 
#  filter(num_rows == 1) %>%
#  select(-num_rows)

dupes <- sdf_hes_ip %>%
# Remove imperfect dupes based on different diagnosis codes - less than 1% of population
  group_by(census2011_person_id, tretspef) %>% 
  count() %>% 
  ungroup()  

sdf_hes_ip <- sdf_hes_ip %>%
  left_join(dupes, by = c("census2011_person_id", "tretspef")) %>%
  filter(n == 1) %>%
  select(-n)

# Update sample flow
if (run_sample_flow == TRUE) {     
  # Update sample flow 
  sample_flow <- add_row(sample_flow,
                         stage = "hes_max_one_row_per_person_per_treatment",
                         count = sdf_nrow(sdf_hes_ip))
  sample_flow <- add_row(sample_flow,
                         stage = "hes_max_one_row_per_person_per_treatment_person_level",
                         count = sdf_nrow(distinct(sdf_hes_ip, census2011_person_id)))
  print(sample_flow)
}

# check max one row per person per treatment
sdf_hes_ip %>% group_by(census2011_person_id, tretspef) %>% count() %>% ungroup() %>% arrange(desc(n)) %>% head()

#--------------------
# Join population data
#---------------------

sdf_population <- sdf_hes_ip %>% 
  left_join(sdf_census2011, by = "census2011_person_id") %>%
  left_join(sdf_deaths, by = "census2011_person_id")

# Update sample flow
if (run_sample_flow == TRUE) {     
  sample_flow <- add_row(sample_flow,
                         stage = "hes_census_deaths",
                         count = sdf_nrow(sdf_population))
  sample_flow <- add_row(sample_flow,
                         stage = "hes_census_deaths_person_level",
                         count = sdf_nrow(distinct(sdf_population, census2011_person_id)))
  print(sample_flow)
}

#---------------------
# Get study population
#---------------------

# usual resident
sdf_population <- filter(sdf_population, usual_resident == "1")

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "usual_resident",
                         count = sdf_nrow(sdf_population))  
  sample_flow <- add_row(sample_flow,
                         stage = "usual_resident_person_level",
                         count = sdf_nrow(distinct(sdf_population, census2011_person_id)))
  print(sample_flow)
}

# living in England
sdf_population <- sdf_population %>%
  mutate(country_code = substr(country_code, 1, 1)) %>%
  filter(country_code == "E")

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "living_in_England",
                         count = sdf_nrow(sdf_population))  
  sample_flow <- add_row(sample_flow,
                         stage = "living_in_England_person_level",
                         count = sdf_nrow(distinct(sdf_population, census2011_person_id)))
  print(sample_flow)
}

# not wholly imputed
sdf_population <- filter(sdf_population, present_in_cenmortlink == 1)

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "not_wholly_imputed",
                         count = sdf_nrow(sdf_population))  
  sample_flow <- add_row(sample_flow,
                         stage = "not_wholly_imputed_person_level",
                         count = sdf_nrow(distinct(sdf_population, census2011_person_id)))
  print(sample_flow)
}

# links to NHS number
sdf_population <- filter(sdf_population, cen_pr_flag == 1)

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "links_to_NHS_number",
                         count = sdf_nrow(sdf_population))  
  sample_flow <- add_row(sample_flow,
                         stage = "links_to_NHS_number_person_level",
                         count = sdf_nrow(distinct(sdf_population, census2011_person_id)))
  print(sample_flow)
}

# alive on study start date
sdf_population <- filter(sdf_population, is.na(dod) | (!is.na(dod) & dod >=  "2015-04-01"))

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "alive_on_study_start_date",
                         count = sdf_nrow(sdf_population))  
  sample_flow <- add_row(sample_flow,
                         stage = "alive_on_study_start_date_person_level",
                         count = sdf_nrow(distinct(sdf_population, census2011_person_id)))
  print(sample_flow)
}

# aged 25 to 64 on index date
sdf_population <- sdf_population %>% 
  get_age_at_index_date() %>%
  dplyr::filter(age_at_index_date >= study_min_age_at_surgery, age_at_index_date < study_max_age_at_surgery)

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "aged_25_to_64_on_index_date",
                         count = sdf_nrow(sdf_population))  
  sample_flow <- add_row(sample_flow,
                         stage = "aged_25_to_64_on_index_date_person_level",
                         count = sdf_nrow(distinct(sdf_population, census2011_person_id)))
  print(sample_flow)
}

# check max one row per person per treatment
sdf_hes_ip %>% group_by(census2011_person_id, tretspef) %>% count() %>% ungroup() %>% arrange(desc(n)) %>% head()
sdf_hes_ip %>% group_by(census2011_person_id) %>% count() %>% ungroup() %>% arrange(desc(n)) %>% head()

#---------------------
# Calendarise population
#---------------------

sdf_study_population_monthly <- get_monthly_population_indicators(sdf = sdf_population,
                                                                  period_start_date = study_period_start,
                                                                  period_end_date = study_period_end,
                                                                  min_age = study_min_age,
                                                                  max_age = study_max_age,
                                                                  id_var = "census2011_person_id",
                                                                  time_of_measurement = "end")

#glimpse(sdf_study_population_monthly)

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "hes_census_deaths_monthly",
                         count = sdf_nrow(sdf_study_population_monthly))  
  sample_flow <- add_row(sample_flow,
                         stage = "hes_census_deaths_monthly_person_level",
                         count = sdf_nrow(distinct(sdf_study_population_monthly, census2011_person_id)))
  print(sample_flow)
}

# Save out temp monthly dataset
spark_write_table(sdf_study_population_monthly, name = "hmrc_outcome.tretspef_hes_data_temp_1504", format = "parquet", mode = "overwrite")
sdf_study_population_monthly <- sdf_sql(sc, paste0("SELECT * FROM hmrc_outcome.tretspef_hes_data_temp_1504"))

#---------------------
# Join NI numbers
#---------------------

# Census IDs may match to multiple NI numbers - we aggregate later
sdf_c11pid_nino_lookup <- sdf_sql(sc, paste0("SELECT * FROM ", lookup_c11pid_nino))

sdf_c11pid_nino_lookup <- sdf_c11pid_nino_lookup %>%
#  filter(cen_id_contains_multiple_ninos_1 == 0) %>%
#  filter(cen_id_contains_multiple_ninos_2 == 0) %>%  
  filter(nino_contains_multiple_census_id_1 == 0) %>%
  filter(nino_contains_multiple_census_id_2 == 0) %>%
  filter(!is.na(census_id)) %>%
  filter(!is.na(frameworks_nino)) %>%
  select(census2011_person_id = census_id,
         encrypted_nino = frameworks_nino)

sdf_c11pid_nino_lookup %>% group_by(census2011_person_id) %>% count() %>% ungroup() %>% arrange(desc(n)) %>% head()
sdf_c11pid_nino_lookup %>% group_by(encrypted_nino) %>% count() %>% ungroup() %>% arrange(desc(n)) %>% head()

sdf_study_population_monthly <- sdf_study_population_monthly %>% 
  left_join(sdf_c11pid_nino_lookup, by = "census2011_person_id")

# Update sample flow
if (run_sample_flow == TRUE) {     
  sample_flow <- add_row(sample_flow,
                         stage = "hes_census_deaths_nino_monthly",
                         count = sdf_nrow(sdf_study_population_monthly))
  sample_flow <- add_row(sample_flow,
                         stage = "hes_census_deaths_nino_monthly_person_level",
                         count = sdf_nrow(distinct(sdf_study_population_monthly, census2011_person_id)))
  print(sample_flow)
}

# links to NI number
sdf_study_population_monthly <- filter(sdf_study_population_monthly, !is.na(encrypted_nino))

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "links_to_NI_number_monthly",
                         count = sdf_nrow(sdf_study_population_monthly))  
  sample_flow <- add_row(sample_flow,
                         stage = "links_to_NI_number_monthly_person_level",
                         count = sdf_nrow(distinct(sdf_study_population_monthly, census2011_person_id)))
  print(sample_flow)
}

sdf_study_population_monthly %>% group_by(census2011_person_id, tretspef, year_month) %>% count() %>% ungroup() %>% arrange(desc(n)) %>% head()

#---------------------
# Join PAYE-RTI data
#---------------------

sdf_paye_rti <- sdf_sql(sc, paste0("SELECT * FROM ", table_paye_rti))

sdf_paye_rti <- sdf_paye_rti %>%
  mutate(year_month = substr(month, 1, 7)) %>%
  select(encrypted_nino = valid_nino,
         pay,
         year_month)

sdf_monthly <- left_join(sdf_study_population_monthly, sdf_paye_rti, by = c("encrypted_nino", "year_month"))

sdf_monthly <- sdf_monthly %>%
  mutate(paid_employee_in_month = ifelse(!is.na(pay) & pay > 0, 1, 0)) %>%
  mutate(pay = ifelse(is.na(pay) | (!is.na(pay) & pay < 0), 0, pay))

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "hes_census_nino_deaths_paye_monthly",
                         count = sdf_nrow(sdf_monthly))  
  sample_flow <- add_row(sample_flow,
                         stage = "hes_census_nino_deaths_paye_monthly_person_level",
                         count = sdf_nrow(distinct(sdf_monthly, census2011_person_id)))
  print(sample_flow)
}

sdf_monthly %>% 
  group_by(year_month, tretspef, census2011_person_id) %>%
  count() %>%
  ungroup() %>%
  arrange(desc(n)) %>%
  head()

# aggregate for people with multiple national insurance numbers
sdf_monthly <- sdf_monthly %>%
  group_by(year_month, census2011_person_id, tretspef) %>%
  mutate(pay = sum(pay, na.rm = TRUE),
         paid_employee_in_month = sum(paid_employee_in_month, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(paid_employee_in_month = ifelse(paid_employee_in_month >= 1, 1, 0)) %>%
  select(-encrypted_nino) %>%
  sdf_drop_duplicates()

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "hes_census_nino_deaths_paye_monthly_aggregated_ni",
                         count = sdf_nrow(sdf_monthly))  
  sample_flow <- add_row(sample_flow,
                         stage = "hes_census_nino_deaths_paye_monthly_aggregated_ni_person_level",
                         count = sdf_nrow(distinct(sdf_monthly, census2011_person_id)))
  print(sample_flow)
}

# check there is max one row per person per month per treatment
sdf_monthly %>% 
  group_by(year_month, tretspef, census2011_person_id) %>%
  count() %>%
  ungroup() %>%
  arrange(desc(n)) %>%
  head()

glimpse(sdf_monthly)

#---------------------
# Join benefits data
#---------------------

# Read in benefits data
benefits_df <- spark_read_table(sc, name = "hmrc_outcome.benefits_table_20240910")

masterkey_lookup <- sdf_sql(sc, "SELECT 
                                 cis_master_key,
                                 census_id
                                 FROM c11_linked_to_dwp_masterkey.census_2011_di_dwpkey_stage_1_2_11012024") 

benefits_df_lookup <- benefits_df %>% 
  left_join(masterkey_lookup, by = c("master_key" = "cis_master_key")) %>%
  mutate(month_start = paste(year, month, "01", sep = "-")) %>% 
  select(-year,-month) %>%
  filter(!is.na(census_id))

# Join study population to benefits data
sdf_monthly <- sdf_monthly %>% 
  left_join(masterkey_lookup, by = c("census2011_person_id" = "census_id"))

# links to benefits cis masterkey
sdf_monthly <- sdf_monthly %>%
  filter(!is.na(cis_master_key)) %>%
  select(-cis_master_key)

sdf_monthly <- sdf_monthly %>%
  left_join(benefits_df_lookup, by = c("census2011_person_id" = "census_id", "month_start"))

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "hes_census_nino_deaths_paye_benefits_monthly",
                         count = sdf_nrow(sdf_monthly))  
  sample_flow <- add_row(sample_flow,
                         stage = "hes_census_nino_deaths_paye_benefits_monthly_person_level",
                         count = sdf_nrow(distinct(sdf_monthly, census2011_person_id)))
  print(sample_flow)
}

# format variables
sdf_monthly <- sdf_monthly %>%
  mutate(has_DLA = ifelse(is.na(has_DLA), 0,has_DLA), 
         has_PC = ifelse(is.na(has_PC), 0,has_PC), 
         has_IB_SDA = ifelse(is.na(has_IB_SDA), 0,has_IB_SDA), 
         has_AA = ifelse(is.na(has_AA), 0,has_AA), 
         has_HB = ifelse(is.na(has_HB), 0,has_HB), 
         has_WB   = ifelse(is.na(has_WB), 0,has_WB), 
         has_PIP  = ifelse(is.na(has_PIP), 0,has_PIP), 
         has_UC = ifelse(is.na(has_UC), 0,has_UC), 
         has_JSA = ifelse(is.na(has_JSA), 0,has_JSA), 
         has_BB  = ifelse(is.na(has_BB), 0,has_BB), 
         has_CA  = ifelse(is.na(has_CA), 0,has_CA), 
         has_RP = ifelse(is.na(has_RP), 0,has_RP), 
         has_ESA = ifelse(is.na(has_ESA), 0,has_ESA), 
         has_IS  = ifelse(is.na(has_IS), 0,has_IS), 
         has_disability_benefit = ifelse(is.na(has_disability_benefit), 0,has_disability_benefit), 
         has_pension_benefit = ifelse(is.na(has_pension_benefit), 0,has_pension_benefit), 
         has_employment_support_benefit = ifelse(is.na(has_employment_support_benefit), 0,has_employment_support_benefit), 
         has_bereavement_benefit = ifelse(is.na(has_bereavement_benefit), 0,has_bereavement_benefit), 
         has_uc_legacy_benefit = ifelse(is.na(has_uc_legacy_benefit), 0,has_uc_legacy_benefit), 
         has_any_benefit  = ifelse(is.na(has_any_benefit), 0,has_any_benefit))

sdf_monthly <- sdf_monthly %>%
  select(-c('has_AA',
            'has_PC',
            'has_BB',
            'has_RP',
            'has_WB',
            'has_pension_benefit',
            'has_bereavement_benefit')) 

sdf_monthly <- sdf_monthly %>%
  mutate(has_any_benefit = 
          case_when(has_DLA == 1 | 
                    has_IB_SDA == 1 |
                    has_HB == 1 |
                    has_PIP == 1 |
                    has_UC == 1 |
                    has_JSA == 1 |
                    has_CA == 1 |
                    has_ESA == 1 |
                    has_IS == 1 ~ 1, TRUE ~ 0),
        has_UC_legacy_new = 
          case_when(has_HB == 1 | 
                    has_UC == 1 | 
                    has_IS == 1 | 
                    has_JSA == 1 ~ 1, TRUE ~ 0),
        has_all_except_disability = 
          case_when(
                    has_IB_SDA == 1 |
                    has_HB == 1 |
                    
                    has_UC == 1 |
                    has_JSA == 1 |
                    has_CA == 1 |
                    has_ESA == 1 |
                    has_IS == 1 ~ 1, TRUE ~ 0))

 sdf_monthly <- sdf_monthly %>%
  select(-c('has_DLA',
            'has_IB_SDA',
            'has_HB',
            'has_PIP',
            'has_UC',
            'has_JSA',
            'has_CA',
            'has_ESA',
            'has_IS'))

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "links_to_cis_master_key_monthly",
                         count = sdf_nrow(sdf_monthly))  
  sample_flow <- add_row(sample_flow,
                         stage = "links_to_cis_master_key_monthly_person_level",
                         count = sdf_nrow(distinct(sdf_monthly, census2011_person_id)))
  print(sample_flow)
}

colnames(sdf_monthly)

sdf_monthly %>% 
  group_by(year_month, tretspef, census2011_person_id) %>%
  count() %>%
  ungroup() %>%
  arrange(desc(n)) %>%
  head()

# aggregate for people with multiple cis masterkeys
sdf_monthly <- sdf_monthly %>%
 group_by(year_month, census2011_person_id, tretspef) %>%
  mutate(has_any_benefit = max(has_any_benefit, na.rm = TRUE),
         has_UC_legacy_new = max(has_UC_legacy_new, na.rm = TRUE),
         has_all_except_disability = max(has_all_except_disability, na.rm = TRUE),
         has_uc_legacy_benefit = max(has_uc_legacy_benefit, na.rm = TRUE),
         has_employment_support_benefit = max(has_employment_support_benefit, na.rm = TRUE),
         has_disability_benefit = max(has_disability_benefit, na.rm = TRUE)) %>% 
  ungroup() %>%
  select(-master_key) %>%
  sdf_drop_duplicates()

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "hes_census_nino_deaths_paye_benefits_monthly_aggregated_cis_masterkey",
                         count = sdf_nrow(sdf_monthly))  
  sample_flow <- add_row(sample_flow,
                         stage = "hes_census_nino_deaths_paye_benefits_monthly_aggregated_cis_masterkey_person_level",
                         count = sdf_nrow(distinct(sdf_monthly, census2011_person_id)))
  print(sample_flow)
}

# check there is max one row per person per month per treatment
sdf_monthly %>% 
  group_by(year_month, tretspef, census2011_person_id) %>%
  count() %>%
  ungroup() %>%
  arrange(desc(n)) %>%
  head()

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "study_population",
                         count = sdf_nrow(sdf_monthly))  
  sample_flow <- add_row(sample_flow,
                         stage = "study_population_person_level",
                         count = sdf_nrow(distinct(sdf_monthly, census2011_person_id)))
  print(sample_flow)
}

#-----------
# Apply filter to age month
#----------- 

# This ensures that the monthly data starts at 30 and ends at 59
# The calendarisation keeps those aged 29.9

sdf_monthly <- sdf_monthly %>% filter(age_month >=30 & age_month < 59)

# Update sample flow
if (run_sample_flow == TRUE) {    
  sample_flow <- add_row(sample_flow,
                         stage = "study_population_filter_30to58",
                         count = sdf_nrow(sdf_monthly))  
  sample_flow <- add_row(sample_flow,
                         stage = "study_population_filter_30to58_person_level",
                         count = sdf_nrow(distinct(sdf_monthly, census2011_person_id)))
  print(sample_flow)
}

#-----------
# Save sample flow
#-----------

write.csv(sample_flow, paste0(cdsw_outputs_folder, "waiting_times_sample_flow_", Sys.Date(), ".csv"),
          row.names = FALSE)

#-----------
# Save table
#-----------

spark_write_table(sdf_monthly, name = "hmrc_outcome.tretspef_hes_data_2804_test", format = "parquet", mode = "overwrite")

tbl_change_db(sc, "hmrc_outcome")
sdf_monthly <- spark_read_table(sc, name = "tretspef_hes_data_2404")


#--------------------------
# Checks
#--------------------------

if (run_checks == TRUE) {

  # check missing
  check_missing <- sdf_monthly %>%
    select(census2011_person_id, tretspef, year_month, 
           pay, paid_employee_in_month, 
           age_at_index_date, admidate, elecdate, elecdur, dod, dor, 
           admimeth, diag_4_01, Name, Group, opertn_4_01, procodet,
           has_any_benefit, has_UC_legacy_new, has_all_except_disability,
           has_uc_legacy_benefit, has_employment_support_benefit, has_disability_benefit) %>%
    mutate_all(is.na) %>%
    mutate_all(as.numeric) %>%
    summarise_all(sum, na.rm = TRUE) %>%
    collect() %>%
    pivot_longer(cols = everything(),
                 names_to = "Variable", 
                 values_to = "Missing") %>%
    arrange(desc(Missing), Variable)
  print(check_missing)

  # total
  total <- sdf_nrow(sdf_monthly) %>% data.frame()
  print(total)
  
  # total person level
  total_person_level <- sdf_nrow(distinct(sdf_monthly, census2011_person_id)) %>% data.frame()
  print(total_person_level)

  # tretspef
  tretspef <- sdf_monthly %>% 
    select(census2011_person_id, tretspef) %>% 
    distinct() %>%
    group_by(tretspef) %>% 
    count() %>% 
    ungroup() %>%
    arrange(tretspef) %>%
    collect()
  print(tretspef)

  # admidate
  admidate <- get_summary_stats(sdf_monthly, "admidate")
  print(admidate)
  
  # elecdate
  elecdate <- get_summary_stats(sdf_monthly, "elecdate")
  print(elecdate)
  
  # elecdur
  elecdur <- get_summary_stats(sdf_monthly, "elecdur")
  print(elecdur)
  
  # admimeth
  admimeth <- sdf_monthly %>% 
    select(census2011_person_id, admimeth) %>% 
    distinct() %>%
    group_by(admimeth) %>% 
    count() %>% 
    ungroup() %>%
    arrange(admimeth) %>%
    collect()
  print(admimeth)
  
  # census starting population
  census_pop <- sdf_monthly %>% 
    group_by(present_in_cenmortlink, cen_pr_flag, country_code, usual_resident) %>%
    count() %>%
    collect()
  print(census_pop)
 
  # death occurrences
  dod <- get_summary_stats(sdf_monthly, "dod")
  print(dod)

  # death registrations
  dor <- get_summary_stats(sdf_monthly, "dor")
  print(dor)

  # age_at_index_date
  age_at_index_date <- sdf_monthly %>% 
    select(census2011_person_id, age_at_index_date) %>% 
    distinct() %>%
    group_by(age_at_index_date) %>% 
    count() %>% 
    ungroup() %>%
    arrange(age_at_index_date) %>%
    collect()
  print(age_at_index_date)
    
  # year_month
  year_month <- get_summary_stats(sdf_monthly, "year_month")
  print(year_month)
  
  # pay
  pay <- get_summary_stats(sdf_monthly, "pay")
  print(pay)
        
  # paid_employee_in_month
  paid_employee_in_month <- sdf_monthly %>% 
    group_by(paid_employee_in_month) %>% 
    count() %>% 
    ungroup() %>%
    arrange(paid_employee_in_month) %>%
    collect()
  print(paid_employee_in_month)
  
  # has_any_benefit
  has_any_benefit <- sdf_monthly %>% 
    group_by(has_any_benefit) %>% 
    count() %>% 
    ungroup() %>%
    arrange(has_any_benefit) %>%
    collect()
  print(has_any_benefit)
  
  # has_UC_legacy_new
  has_UC_legacy_new <- sdf_monthly %>% 
    group_by(has_UC_legacy_new) %>% 
    count() %>% 
    ungroup() %>%
    arrange(has_UC_legacy_new) %>%
    collect()
  print(has_UC_legacy_new)

  # has_all_except_disability
  has_all_except_disability <- sdf_monthly %>% 
    group_by(has_all_except_disability) %>% 
    count() %>% 
    ungroup() %>%
    arrange(has_all_except_disability) %>%
    collect()
  print(has_all_except_disability)
  
  # has_uc_legacy_benefit
  has_uc_legacy_benefit <- sdf_monthly %>% 
    group_by(has_uc_legacy_benefit) %>% 
    count() %>% 
    ungroup() %>%
    arrange(has_uc_legacy_benefit) %>%
    collect()
  print(has_uc_legacy_benefit)
  
  # has_employment_support_benefit
  has_employment_support_benefit <- sdf_monthly %>% 
    group_by(has_employment_support_benefit) %>% 
    count() %>% 
    ungroup() %>%
    arrange(has_employment_support_benefit) %>%
    collect()
  print(has_employment_support_benefit)

  # has_disability_benefit
  has_disability_benefit <- sdf_monthly %>% 
    group_by(has_disability_benefit) %>% 
    count() %>% 
    ungroup() %>%
    arrange(has_disability_benefit) %>%
    collect()
  print(has_disability_benefit)
 
  # Save dataset checks
  dataset_checks <- list(check_missing, total, total_person_level, tretspef, admidate, elecdate, elecdur,
                         admimeth, census_pop, dod, dor, age_at_index_date, year_month,
                         pay, paid_employee_in_month,
                         has_any_benefit, has_UC_legacy_new, has_all_except_disability,
                         has_uc_legacy_benefit, has_employment_support_benefit, has_disability_benefit)

  names(dataset_checks) <- c("check_missing", "total", "total_person_level", "tretspef", "admidate", "elecdate", "elecdur",
                             "admimeth", "census_pop", "dod", "dor", "age_at_index_date", "year_month",
                             "pay", "paid_employee_in_month",
                             "has_any_benefit", "has_UC_legacy_new", "has_all_except_disability",
                             "has_uc_legacy_benefit", "has_employment_support_benefit", "has_disability_benefit")

  writexl::write_xlsx(dataset_checks, paste0(cdsw_outputs_folder, "waiting_times_dataset_checks_", Sys.Date(), ".xlsx"))  
  
}

#-----------
# Plots
#-----------

# age at index date
age_plot <- sdf_monthly %>%
  select(census2011_person_id, age_at_index_date) %>%
  distinct() %>%
  mutate(age_at_index_date = floor(age_at_index_date)) %>%
  group_by(age_at_index_date) %>%
  count() %>%
  arrange(age_at_index_date) %>%
  collect()

write.csv(age_plot, paste0(cdsw_outputs_folder, "waiting_times_plot_age_at_index_date.csv"),
          row.names = FALSE)

ggplot(data = age_plot, 
       aes(x = age_at_index_date, 
           y = n)) +
  geom_bar(stat = "identity", position = position_dodge(width = 0.8)) +
  labs(title = "Age on index date",
       x = "Age on index date",
       y = "Count") +
  scale_y_continuous(limits = c(0, NA), labels = scales::comma) +
  theme_bw()

ggsave(paste0(cdsw_outputs_folder, "waiting_times_plot_age_at_index_date.pdf"), 
       width = 10, height = 4, units = "in")

filter_list <- sort(c("320", "170", "330", "120", 
                      "430", "301", "300", "100", 
                      "502", "400", "150", "130", 
                      "140", "160", "340", "410",
                      "110", "101", "710"))


for (i in filter_list) {
  
  age_plot <- sdf_monthly %>%
    filter(tretspef == i) %>%
    select(census2011_person_id, age_at_index_date) %>%
    distinct() %>%
    mutate(age_at_index_date = floor(age_at_index_date)) %>%
    group_by(age_at_index_date) %>%
    count() %>%
    arrange(age_at_index_date) %>%
    collect()

  write.csv(age_plot, paste0(cdsw_outputs_folder, "waiting_times_plot_age_at_index_date_", i, ".csv"),
            row.names = FALSE)

  ggplot(data = age_plot, 
         aes(x = age_at_index_date, 
             y = n)) +
    geom_bar(stat = "identity", position = position_dodge(width = 0.8)) +
    labs(title = "Age on index date",
         x = "Age on index date",
         y = "Count") +
    scale_y_continuous(limits = c(0, NA), labels = scales::comma) +
    theme_bw()

  ggsave(paste0(cdsw_outputs_folder, "waiting_times_plot_age_at_index_date_", i, ".pdf"), 
         width = 10, height = 4, units = "in")

}

# treatment
tretspef_plot <- sdf_monthly %>%
  select(census2011_person_id, tretspef) %>%
  distinct() %>%
  group_by(tretspef) %>%
  count() %>%
  arrange(tretspef) %>%
  collect()

write.csv(tretspef_plot, paste0(cdsw_outputs_folder, "waiting_times_plot_tretspef.csv"),
          row.names = FALSE)

ggplot(data = tretspef_plot, 
       aes(x = tretspef, 
           y = n)) +
  geom_bar(stat = "identity", position = position_dodge(width = 0.8)) +
  labs(title = "Treatment",
       x = "Treatment code",
       y = "Count") +
  scale_y_continuous(limits = c(0, NA), labels = scales::comma) +
  theme_bw()

ggsave(paste0(cdsw_outputs_folder, "waiting_times_plot_tretspef.pdf"), 
       width = 10, height = 4, units = "in")


# treatment subset
tretspef_plot_subset <- sdf_monthly %>%
  filter(tretspef %in% filter_list) %>%
  select(census2011_person_id, tretspef) %>%
  distinct() %>%
  group_by(tretspef) %>%
  count() %>%
  arrange(tretspef) %>%
  collect()

write.csv(tretspef_plot_subset, paste0(cdsw_outputs_folder, "waiting_times_plot_tretspef_subset.csv"),
          row.names = FALSE)

ggplot(data = tretspef_plot_subset, 
       aes(x = tretspef, 
           y = n)) +
  geom_bar(stat = "identity", position = position_dodge(width = 0.8)) +
  labs(title = "Treatment",
       x = "Treatment code",
       y = "Count") +
  scale_y_continuous(limits = c(0, NA), labels = scales::comma) +
  theme_bw()

ggsave(paste0(cdsw_outputs_folder, "waiting_times_plot_tretspef_subset.pdf"), 
       width = 10, height = 4, units = "in")


# waiting times
for (i in filter_list) {
  
  elecdur_plot <- sdf_monthly %>%
    filter(tretspef == i) %>%
    filter(elecdur <= 365) %>%
    select(census2011_person_id, elecdur) %>%
    distinct() %>%
    group_by(elecdur) %>%
    count() %>%
    ungroup() %>%
    mutate(percentage = n / sum(n) * 100) %>%
    arrange(elecdur) %>%
    collect()

  write.csv(elecdur_plot, paste0(cdsw_outputs_folder, "waiting_times_plot_elecdur_", i, ".csv"),
            row.names = FALSE)

  ggplot(data = elecdur_plot, 
         aes(x = elecdur, 
             y = percentage)) +
    geom_bar(stat = "identity", position = position_dodge(width = 0.8)) +
    geom_vline(xintercept = 126, colour = "red") +
    labs(title = "Waiting time",
         x = "Waiting time (days)",
         y = "Percentage") +
    scale_y_continuous(limits = c(0, NA), labels = scales::comma) +
    theme_bw()

  ggsave(paste0(cdsw_outputs_folder, "waiting_times_plot_elecdur_", i, ".pdf"), 
         width = 10, height = 4, units = "in")

}

# waiting time summary stats
tretspef_list <- sdf_monthly %>%
  select(tretspef) %>%
  distinct() %>%
  arrange(tretspef) %>%
  pull()

elecdur_summary <- data.frame()

for (i in tretspef_list) {
  
  elecdur_summary_i <- sdf_monthly %>%
    filter(tretspef == i) %>%
    select(census2011_person_id, elecdur) %>%
    distinct() %>%
    summarise(min_elecdur = min(elecdur, na.rm = TRUE),
              q1_elecdur = quantile(elecdur, 0.25),
              average_elecdur = mean(elecdur, na.rm = TRUE),
              median_elecdur = quantile(elecdur, 0.5),
              q3_elecdur = quantile(elecdur, 0.75),
              max_elecdur = max(elecdur, na.rm = TRUE),
              n_people = n()) %>%
    mutate(tretspef = i) %>%
    relocate(tretspef, .before = everything()) %>%
    data.frame()

  elecdur_summary <- bind_rows(elecdur_summary, elecdur_summary_i)
  
}

write.csv(elecdur_summary, paste0(cdsw_outputs_folder, "waiting_times_elecdur_summary.csv"),
          row.names = FALSE)


elecdur_summary <- data.frame()

for (i in filter_list) {
  
  elecdur_summary_i <- sdf_monthly %>%
    filter(tretspef == i) %>%
    select(census2011_person_id, elecdur) %>%
    distinct() %>%
    summarise(min_elecdur = min(elecdur, na.rm = TRUE),
              q1_elecdur = quantile(elecdur, 0.25),
              average_elecdur = mean(elecdur, na.rm = TRUE),
              median_elecdur = quantile(elecdur, 0.5),
              q3_elecdur = quantile(elecdur, 0.75),
              max_elecdur = max(elecdur, na.rm = TRUE),
              n_people = n()) %>%
    mutate(tretspef = i) %>%
    relocate(tretspef, .before = everything()) %>%
    data.frame()

  elecdur_summary <- bind_rows(elecdur_summary, elecdur_summary_i)
  
}

write.csv(elecdur_summary, paste0(cdsw_outputs_folder, "waiting_times_elecdur_summary_subset.csv"),
          row.names = FALSE)




#-------------#
# END OF FILE #
#-------------#



