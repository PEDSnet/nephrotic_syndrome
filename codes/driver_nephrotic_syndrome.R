##################################################################################
# GLOMERULAR DISEASE COHORTS ----
message("Identify glomerular disease cohorts")

# Follow up requirements from skeletal outcomes project
# (Patients with >=2 outpatient visits separated by at least one year)
so_follow_up_req <-
  cdm_tbl('visit_occurrence') %>%
  filter(!site %in% c('lurie','stanford','national','texas')) %>%
  filter(visit_start_date >= as.Date("2010-01-01"),
         visit_start_date < as.Date("2022-12-01")) %>%
  filter(visit_concept_id == 9202L,
         visit_start_age_in_months < (12 * 22L),
         visit_start_age_in_months >= (12 * 2L)) %>%
  group_by(person_id) %>%
  summarize(
    min_visit = min(visit_start_date),
    max_visit = max(visit_start_date)
  ) %>%
  ungroup() %>%
  filter(max_visit - min_visit >= 365) %>% 
  compute_new()

so_follow_up_req %>%
  add_site() %>%
  filter(!site %in% c('lurie','stanford','national','texas')) %>%
  output_tbl("so_follow_up_req")

#Initialize list
init_sum(cohort = 'Start', persons = 0)
rslt <- list()
# Glomerular disease phenotype from skeletal outcomes project
# RUN GLEAN algorithm, IF IT HAS NOT ALREADY BEEN RUN
run_glean_alg_sql(db = config("db_src")) # Note that SQL code has to be updated manually with new results schemas


so_glean_cohort <- results_tbl("so_glean_cohort", results_tag = FALSE) %>%
  inner_join(physician_visit, by='person_id') %>%
  filter(!site %in% c('lurie','stanford','national','texas')) %>%
  output_tbl('so_glean_cohort_siteexcl')


##################################################################################
# # Glomerular disease codeset
glomerular_disease <-
  load_codeset("glomerular_disease", col_types = "iccciciiii")

glomerular_disease_other_code_req_vctr <-
  glomerular_disease %>%
  filter(other_code_req == 1) %>%
  select(concept_id) %>%
  pull()

glomerular_disease_neph_req_vctr <-
  glomerular_disease %>%
  filter(neph_req == 1) %>%
  select(concept_id) %>% pull()

glomerular_disease_w_gen_flag <- glomerular_disease %>%
  filter(genetic_flag == 1 | congenital_flag == 1)
# 
# # Identify glomerular disease cohort
# # 2 broad pathways into the cohort:
# # -- 2 or more glomerular inclusion diagnoses on different dates
# # -- 1 or more glomerular inclusion diagnosis AND 1 or more kidney biopsy which is not post-transplant
# # 2 glomerular inclusion diagnosis groups had special requirements:
# # -- "other_code_req" inclusion diagnoses alone are not sufficient for a patient to meet the computational phenotype:
# # these codes are only included if they have 1 or more other glomerular inclusion diagnoses
# # (i.e. NOT from "other_code_req" list) on a different date or if they have 1 or more kidney biopsy which is not post-transplant
# # -- "neph_req" diagnoses are only included for the algorithm if associated with a nephrology visit
# 
# message("Identify glomerular disease cohort 2010 to 2022")
# 
#  glom_dx_cohort_20102022 <-
#   get_glean_cohort(
#     condition_tbl = condition_occurrence_20102022,
#     procedure_tbl = procedure_occurrence_20102022,
#     visit_tbl = visit_occurrence_20102022,
#     kidney_transplant_proc_codeset = load_codeset("kidney_transplant_proc"),
#     kidney_biopsy_proc_codeset = load_codeset("kidney_biopsy_proc"),
#     biopsy_proc_codeset = load_codeset("biopsy_proc"),
#     glomerular_disease_codeset = glomerular_disease,
#     other_code_req_vctr = glomerular_disease_other_code_req_vctr,
#     neph_req_vctr = glomerular_disease_neph_req_vctr,
#     nephrology_spec_codeset  = load_codeset("nephrology_specialty")
#   ) %>%
#   inner_join(physician_visit, by = "person_id")
# 
# glom_dx_cohort_20102022 %>% output_tbl("glom_dx_cohort_20102022",
#                                        indexes = list("person_id"))
# 
# append_sum(cohort = "glom_dx_cohort_20102022",
#            persons = glom_dx_cohort_20102022 %>% distinct_ct())


##################################################################################

# NEPHROTIC SUBCOHORT ----
message("Identify nephrotic subcohorts")

# Among glomerular disease cohort, identify the nephrotic subcohort -
# patients with more inclusion diagnoses categorized as nephrotic than
# inclusion diagnoses marked as nephritic

message("Identify nephrotic subcohort 2010 to 2022")

# Nephrotic disease from skeletal outcomes project
so_nephrotic_sc <-
  get_nephrotic_subcohort_so(
    cohort = results_tbl('so_glean_cohort_siteexcl'),
    cond_tbl = cdm_tbl("condition_occurrence"),
    min_cutoff_date='2010-01-01',
    max_cutoff_date='2022-11-30',
    ns_codeset = load_codeset("snomed_condition_list_ns", col_types = "icii") %>% filter(ns == 1),
    nephritis_codeset = load_codeset("snomed_condition_list_ns", col_types = "icii") %>% filter(nephritis == 1)
  ) %>%
  filter(!site %in% c('lurie','stanford','national','texas')) %>%
  compute_new()

so_nephrotic_sc %>% output_tbl("so_nephrotic_sc")

# nephrotic_20102022 <-
#   get_nephrotic_subcohort(
#     condition_tbl = condition_occurrence_20102022,
#     cohort = glom_dx_cohort_20102022,
#     nephrotic_codeset = glomerular_disease %>% filter(codeset_category_code == 1),
#     nephritic_codeset = glomerular_disease %>% filter(codeset_category_code == 2)
#   )
# 
# nephrotic_20102022 %>% output_tbl("nephrotic_20102022",
#                                   indexes = list("person_id"))
# 
# append_sum(cohort = "nephrotic_20102022",
#            persons = nephrotic_20102022 %>% distinct_ct())


##################################################################################

# Remove patients with at least one diagnosis flagged as genetic or congenital ----
message("Remove patients with genetic/congenital dx")

message("Identify patients with genetic/congenital dx")
gen_to_excl <-
  get_conditions(
    provided_cohort = results_tbl('so_nephrotic_sc'),
    condition_codeset = glomerular_disease_w_gen_flag,
    condition_tbl = cdm_tbl("condition_occurrence")
  ) %>%
  distinct(person_id)

nephrotic_gen_excl <- results_tbl('so_nephrotic_sc') %>%
  anti_join(gen_to_excl, by = "person_id")

nephrotic_gen_excl %>% output_tbl("nephrotic_gen_excl",
                                  indexes = list("person_id"))

append_sum(cohort = "nephrotic_gen_excl",
           persons = nephrotic_gen_excl %>% distinct_ct())

##################################################################################

### SLE COHORT ----

### Identify SLE cohort which will be applied as an exclusion criterion
message("Get conditions for patients with any SLE inclusion diagnosis and no
          exclusion diagnosis")
elig_conds_sle_i_e_pats <-
  get_elig_conds_for_i_e_pats(incl_codeset_name = "codeset_sle_incl",
                              excl_codeset_name = "codeset_sle_excl",
                              cond_tbl=cdm_tbl('condition_occurrence'))

message("Get patients with any SLE inclusion diagnosis and no
          exclusion diagnosis")
sle_i_e_pats <- elig_conds_sle_i_e_pats %>%
  distinct(person_id, site) %>%
  compute_new(name = "sle_i_e_pats")

append_sum(cohort = "sle_i_e_pats",
           persons = distinct_ct(sle_i_e_pats))

message(
  "Get eligible conditions for patients with any SLE inclusion diagnosis
          and no exclusion diagnosis and 2 or more in-person visits with a
          nephrology or rheumatology care_site or provider"
)
elig_conds_sle_i_e_pats_w_elig_visits <-
  get_elig_conds_for_i_e_pats_w_elig_visits(
    incl_spec_codeset = load_codeset("codeset_rheum_nephr_spec"),
    incl_visit_codeset = load_codeset("in_person"),
    elig_conds_for_i_e_pats = elig_conds_sle_i_e_pats
  )

message(
  "Get patients with any SLE inclusion diagnosis
          and no exclusion diagnosis and 2 or more in-person visits with a
          nephrology or rheumatology care_site or provider"
)
sle_i_e_pats_w_elig_visits <-
  elig_conds_sle_i_e_pats_w_elig_visits %>%
  distinct(person_id, site) %>%
  compute_new(name = "sle_i_e_pats_w_elig_visits")

append_sum(cohort = "sle_i_e_pats_w_elig_visits",
           persons = distinct_ct(sle_i_e_pats_w_elig_visits))

message("Get patients who meet criteria for SLE algorithm")
sle_cohort <-
  get_sle_cohort(
    elig_conds_sle_i_e_pats_w_elig_visits =
      elig_conds_sle_i_e_pats_w_elig_visits,
    drug_codeset = load_codeset("codeset_hydroxychloroquine")
  )

sle_cohort %>% output_tbl(name = "sle_cohort",
                          indexes = list("person_id"))

append_sum(cohort = "sle_cohort",
           persons = distinct_ct(sle_cohort))

##################################################################################

### SLE EXCLUSION ----
message(
  "Apply SLE exclusion criterion to nephrotic subcohort
          with genetic exclusion already applied"
)

nephrotic_no_sle <-
  nephrotic_gen_excl %>%
  anti_join(sle_cohort, by = "person_id")

nephrotic_no_sle %>% output_tbl("nephrotic_no_sle",
                                indexes = list("person_id"))

append_sum(cohort = "nephrotic_no_sle",
           persons = distinct_ct(nephrotic_no_sle))

