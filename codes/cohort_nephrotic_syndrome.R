run_glean_alg_sql <- function(db) {
  # big_codeset_vid_dates ----
  dbExecute(
    db,
    '
  create table nephrotic_anti_cd20.big_codeset_vid_dates as
  select distinct person_id, visit_occurrence_id, condition_start_date, \'no_flag\' as problem_list_flag
  from dcc_pedsnet.condition_occurrence
  where condition_concept_id in
    (
      4298809, 4125958, 4286024, 4128061, 4161421, 4260398, 4030513, 4263367, 4059452, 195289, 4027120, 4093431, 4222610, 4236844, 4056346, 4056478, 4125954, 4125955, 4128055, 4172011, 4241966, 4294813, 4008560, 4030514, 4056462, 4058840, 4058843, 4128065, 193253, 195314, 252365, 312358, 199071, 194405, 192364, 433257, 442075, 435320, 197319, 192362, 259070, 442074, 196464, 435003, 442076
    )
  and condition_type_concept_id <> 38000245
  and condition_start_age_in_months < 360

  union

  select distinct person_id, visit_occurrence_id, condition_start_date, \'flag\' as problem_list_flag
  from dcc_pedsnet.condition_occurrence
  where condition_concept_id in
    (
      4298809, 4125958, 4286024, 4128061, 4161421, 4260398, 4030513, 4263367, 4059452, 195289, 4027120, 4093431, 4222610, 4236844, 4056346, 4056478, 4125954, 4125955, 4128055, 4172011, 4241966, 4294813, 4008560, 4030514, 4056462, 4058840, 4058843, 4128065, 193253, 195314, 252365, 312358, 199071, 194405, 192364, 433257, 442075, 435320, 197319, 192362, 442074, 196464, 435003, 442076, 259070
    )
  and condition_type_concept_id = 38000245
  and condition_start_age_in_months < 360;
  '
  )
  
  # acute_gn_vid_dates ----
  dbExecute(
    db,
    '
  create table nephrotic_anti_cd20.acute_gn_vid_dates as
  select distinct co.person_id, vo.visit_occurrence_id, co.condition_start_date, \'flag\' as problem_list_flag
  from dcc_pedsnet.condition_occurrence co
  left join dcc_pedsnet.visit_occurrence vo
    on co.visit_occurrence_id = vo.visit_occurrence_id
  left join dcc_pedsnet.care_site cs
    on cs.care_site_id = vo.care_site_id
  left join dcc_pedsnet.provider p
    on co.provider_id = p.provider_id
  where co.condition_concept_id in (435308, 259070)
  and
    (
      cs.specialty_concept_id in (45756813, 38004479)
      or
      p.specialty_concept_id in (45756813, 38004479)
    )
  and co.condition_start_age_in_months < 360
  and condition_type_concept_id = 38000245

  union

  select distinct co.person_id, vo.visit_occurrence_id, co.condition_start_date, \'no_flag\' as problem_list_flag
  from dcc_pedsnet.condition_occurrence co
  left join dcc_pedsnet.visit_occurrence vo
    on co.visit_occurrence_id = vo.visit_occurrence_id
  left join dcc_pedsnet.care_site cs
    on cs.care_site_id = vo.care_site_id
  left join dcc_pedsnet.provider p
    on co.provider_id = p.provider_id
  where co.condition_concept_id in (435308)
  and
    (
      cs.specialty_concept_id in (45756813, 38004479)
      or
      p.specialty_concept_id in (45756813, 38004479)
    )
  and co.condition_start_age_in_months < 360
  and condition_type_concept_id <> 38000245;
  '
  )
  
  # has_one_diag ----
  dbExecute(
    db,
    '
    create table nephrotic_anti_cd20.has_one_diag as
    select person_id, count(distinct condition_start_date)
    from dcc_pedsnet.condition_occurrence
    where condition_concept_id in
    (
      4298809, 4125958, 4286024, 4128061, 4161421, 4260398, 4030513, 4263367, 4059452, 195289, 4027120, 4093431, 4222610, 4236844, 4056346, 4056478, 4125954, 4125955, 4128055, 4172011, 4241966, 4294813, 4008560, 4030514, 4056462, 4058840, 4058843, 4128065, 193253, 195314, 252365, 312358, 199071, 194405, 192364, 433257, 442075, 435320, 197319, 192362, 442074, 196464, 435003, 442076
    )
    group by person_id
    having count(distinct condition_start_date) = 1;
      '
  )
  
  # gs_vid_dates ----
  dbExecute(
    db,
    '
    create table nephrotic_anti_cd20.gs_vid_dates as
    select distinct person_id, visit_occurrence_id, condition_start_date, \'no_flag\' as problem_list_flag
    from dcc_pedsnet.condition_occurrence
    where condition_concept_id in
    (
      261071
    )
    and condition_type_concept_id <> 38000245
    and condition_start_age_in_months < 360
    and person_id in (select person_id from nephrotic_anti_cd20.has_one_diag)

    union

    select distinct person_id, visit_occurrence_id, condition_start_date, \'flag\' as problem_list_flag
    from dcc_pedsnet.condition_occurrence
    where condition_concept_id in
    (
      261071
    )
    and condition_type_concept_id = 38000245
    and condition_start_age_in_months < 360
    and person_id in (select person_id from nephrotic_anti_cd20.has_one_diag)
    '
  )
  
  # biglist_no_an_gs ----
  dbExecute(
    db,
    '
  create table nephrotic_anti_cd20.biglist_no_an_gs as
  select case when a.person_id is not null then a.person_id else b.person_id end as person_id, coalesce(non_problem_list_count, 0) as non_problem_list_count,
        coalesce(problem_list_count, 0) as problem_list_count from
          (
            select person_id, count(distinct coalesce(visit_occurrence_id, 1)) as non_problem_list_count
              from
                (
                  select person_id, visit_occurrence_id from nephrotic_anti_cd20.big_codeset_vid_dates
                  where problem_list_flag = \'no_flag\'
                ) a
            group by person_id

          ) a -- Big Code List, NOT on problem list

        full join

          (
            select person_id, count(distinct condition_start_date) as problem_list_count
            from
            (
              select person_id, condition_start_date
              from nephrotic_anti_cd20.big_codeset_vid_dates
              where problem_list_flag = \'flag\'

              except

              select person_id, condition_start_date
              from nephrotic_anti_cd20.big_codeset_vid_dates
              where problem_list_flag = \'no_flag\'
            ) a
            group by person_id
          ) b -- Big Code List, YES on problem list
      on a.person_id = b.person_id;
    '
  )
  
  # an_list ----
  dbExecute(
    db,
    '
  create table nephrotic_anti_cd20.an_list as
  select case when a.person_id is not null then a.person_id else b.person_id end as person_id, coalesce(non_problem_list_count, 0) as non_problem_list_count,
        coalesce(problem_list_count, 0) as problem_list_count from
          (
            select person_id, count(distinct coalesce(visit_occurrence_id, 1)) as non_problem_list_count
              from
                (
                  select a.person_id, a.visit_occurrence_id from nephrotic_anti_cd20.acute_gn_vid_dates a
                  left join nephrotic_anti_cd20.big_codeset_vid_dates b
                    on a.person_id = b.person_id and a.condition_start_date = b.condition_start_date
                  where a.problem_list_flag = \'no_flag\'
                  and b.condition_start_date is null
                  -- and b.problem_list_flag = \'flag\'
                  except
                    (
                      select person_id, visit_occurrence_id from nephrotic_anti_cd20.big_codeset_vid_dates
                    )
                ) a
              group by person_id

          ) a -- Big Code List, NOT on problem list

        full join
          (
            select person_id, count(distinct condition_start_date) as problem_list_count
            from
            (
              select person_id, condition_start_date from nephrotic_anti_cd20.acute_gn_vid_dates
              where problem_list_flag = \'flag\'

              except
                (
                  select person_id, condition_start_date from nephrotic_anti_cd20.big_codeset_vid_dates
                  union
                  select person_id, condition_start_date from nephrotic_anti_cd20.acute_gn_vid_dates
                    where problem_list_flag = \'no_flag\'
                )

            ) a
            group by person_id
          ) b -- Big Code List, YES on problem list
      on a.person_id = b.person_id;
    '
  )
  
  # gs_list ----
  dbExecute(
    db,
    '
  create table nephrotic_anti_cd20.gs_list as
  select case when a.person_id is not null then a.person_id else b.person_id end as person_id, coalesce(non_problem_list_count, 0) as non_problem_list_count, coalesce(problem_list_count, 0) as problem_list_count from
    (
      select person_id, count(distinct coalesce(visit_occurrence_id, 1)) as non_problem_list_count
      from
        (
          select v.person_id, visit_occurrence_id
          from nephrotic_anti_cd20.gs_vid_dates v
          left join
            (
              select person_id, condition_start_date from nephrotic_anti_cd20.big_codeset_vid_dates where problem_list_flag = \'flag\'
                union
              select person_id, condition_start_date from nephrotic_anti_cd20.acute_gn_vid_dates where problem_list_flag = \'flag\'
            ) c on v.person_id = c.person_id and v.condition_start_date = c.condition_start_date
          where v.problem_list_flag = \'no_flag\'
          and c.condition_start_date is null
          except
            (
              select person_id, visit_occurrence_id from nephrotic_anti_cd20.big_codeset_vid_dates
              union
              select person_id, visit_occurrence_id from nephrotic_anti_cd20.acute_gn_vid_dates
            )
        ) b
      group by person_id

    ) a -- GS List, NOT on problem list

  full join

    (
      select person_id, count(distinct condition_start_date) as problem_list_count
      from
      (
        select person_id, condition_start_date
        from nephrotic_anti_cd20.gs_vid_dates
        where problem_list_flag = \'flag\'

        except
        (
          select person_id, condition_start_date from nephrotic_anti_cd20.big_codeset_vid_dates
          union
          select person_id, condition_start_date from nephrotic_anti_cd20.acute_gn_vid_dates
          union
          select person_id, condition_start_date from nephrotic_anti_cd20.gs_vid_dates
          where problem_list_flag = \'no_flag\'
        )
      ) a
      group by person_id
    ) b -- GS List, YES on problem list
  on a.person_id = b.person_id
    '
  )
  
  # combined_table_visits ----
  dbExecute(
    db,
    '
  create table nephrotic_anti_cd20.combined_table_visits as
        select case when bl.person_id is not null then bl.person_id
        when bl.person_id is null and an.person_id is not null then an.person_id
        else gs.person_id end as person_id,
        bl.non_problem_list_count as biglist_npl, bl.problem_list_count as biglist_pl,
        an.non_problem_list_count as an_npl, an.problem_list_count as an_pl,
        gs.non_problem_list_count as gs_npl, gs.problem_list_count as gs_pl,
        coalesce(bl.non_problem_list_count, 0) +
          coalesce(bl.problem_list_count, 0) +
          coalesce(an.non_problem_list_count,0) +
          coalesce(an.problem_list_count, 0) +
          coalesce(gs.non_problem_list_count, 0) +
          coalesce(gs.problem_list_count, 0) as total_visit_count
        from nephrotic_anti_cd20.biglist_no_an_gs bl
        full join nephrotic_anti_cd20.an_list an
        on bl.person_id = an.person_id
        full join nephrotic_anti_cd20.gs_list gs
        on an.person_id = gs.person_id;
    '
  )
  
  # all_biopsies ----
  dbExecute(
    db,
    '
  create table nephrotic_anti_cd20.all_biopsies as
  select a.person_id, biopsy_date, transplant_date from
  (
    select distinct p.person_id, min(p.procedure_date) as biopsy_date
    from dcc_pedsnet.condition_occurrence c
    inner join dcc_pedsnet.procedure_occurrence p
    on c.person_id = p.person_id
    where c.condition_concept_id in
    (
      259070, 4298809, 4125958, 4286024, 4128061, 4161421, 4260398, 4030513, 4263367, 4059452, 195289, 4027120, 4093431, 4222610, 4236844, 4056346, 4056478, 4125954, 4125955, 4128055, 4172011, 4241966, 4294813, 4008560, 4030514, 4056462, 4058840, 4058843, 4128065, 193253, 195314, 252365, 312358, 199071, 194405, 192364, 433257, 442075, 435320, 197319, 192362, 442074, 196464, 435003, 442076, 261071, 435308
    )
    and
    (
      p.procedure_concept_id in (2109566, 2003588)
      or
      (
        lower(procedure_source_value) like \'%renal%\' and procedure_concept_id = 2211783
      )
    )
    and p.procedure_age_in_months < 360
    group by p.person_id
  ) a

  left join

  (
    select distinct p.person_id, min(p.procedure_date) as transplant_date
    from dcc_pedsnet.procedure_occurrence p
    where p.procedure_concept_id in
    (
      2003626, 45887600, 2109587, 2109586
    )
    and p.procedure_age_in_months < 360
    group by p.person_id
  ) b
  on a.person_id = b.person_id;
    '
  )
  
  # glean_cohort ----
  dbExecute(
    db,
    '
  create table nephrotic_anti_cd20.so_glean_cohort as
  with person_list as
    (
      select person_id from nephrotic_anti_cd20.combined_table_visits
        where total_visit_count >= 2
        union
        (
          select person_id from nephrotic_anti_cd20.all_biopsies where biopsy_date < transplant_date
          union
          select person_id from nephrotic_anti_cd20.all_biopsies where transplant_date is null
        )
    )
  select g.person_id, p.site
  from person_list g
  inner join dcc_pedsnet.person p
    on g.person_id = p.person_id
  where g.person_id not in
    (select person_id from dcc_pedsnet.death);
    '
  )
}







#' Get nephrotic subcohort
#' 
#' Get members of GLEAN cohort who have more nephrotic syndrome diagnoses than
#' nephritis diagnoses
#'
#' @param cohort GLEAN cohort
#' @param ns_codeset Nephrotic syndrome codeset
#' @param nephritis_codeset Nephritis codeset
#'
#' @return Patients in GLEAN cohort who have more nephrotic syndrome diagnoses
#'  than nephritis diagnoses
#' 
get_nephrotic_subcohort_so <- function(cohort,
                                    ns_codeset,
                                    nephritis_codeset,
                                    min_cutoff_date='2010-01-01',
                                    max_cutoff_date='2020-12-31',
                                    cond_tbl=cdm_tbl('condition_occurrence')) {
  
  conditions_glean <- cond_tbl %>%
    filter(as.Date(condition_start_date) >= as.Date(min_cutoff_date)) %>%
    filter(as.Date(condition_start_date) <= as.Date(max_cutoff_date)) %>%
    inner_join(select(cohort, person_id), by = "person_id")
  
  cohort_ns_dx <- conditions_glean %>%
    inner_join(select(ns_codeset, concept_id),
               by = c("condition_concept_id" = "concept_id")) %>%
    group_by(person_id) %>%
    summarise(n_ns_dx = n_distinct(condition_start_date)) %>%
    ungroup()
  
  cohort_nephritis_dx <- conditions_glean %>%
    inner_join(
      select(nephritis_codeset, concept_id),
      by = c("condition_concept_id" = "concept_id")
    ) %>%
    group_by(person_id) %>%
    summarise(n_nephritis_dx = n_distinct(condition_start_date)) %>%
    ungroup()
  
  ns_nephritis_counts <- cohort %>%
    select(person_id) %>%
    left_join(cohort_ns_dx, by = "person_id") %>%
    left_join(cohort_nephritis_dx, by = "person_id") %>%
    mutate(
      n_ns_dx = if_else(is.na(n_ns_dx), 0, n_ns_dx),
      n_nephritis_dx = if_else(is.na(n_nephritis_dx), 0, n_nephritis_dx)
    )
  
  ns_nephritis_counts %>% 
    filter(n_ns_dx > n_nephritis_dx) %>%
    add_site()
  
}


get_biopsy_procedures <- function(cohort,
                                  kb_codeset,
                                  procedure_tbl) {
  kb_cis <-
    kb_codeset %>%
    select(concept_id) %>%
    pull()
  
  kb_procedures <- procedure_tbl %>%
    filter(procedure_concept_id %in% kb_cis) %>%
    inner_join(cohort, by = "person_id") %>%
    inner_join(kb_codeset, by = c("procedure_concept_id" = "concept_id")) %>%
    filter(
      sv_search_req == 0 |
        sv_search_req == 1 &
        !str_detect(tolower(procedure_source_value), "adrenal"),
      (
        str_detect(tolower(procedure_source_value), "renal") |
          str_detect(tolower(procedure_source_value), "kidney")
      )
    )
}


#' Get table of conditions in codeset, restricted to cohort if provided,
#' from CDM condition table
#'
#' @param provided_cohort Cohort
#' @param condition_codeset Condition codeset
#'
#' @return Table of conditions in codeset, restricted to
#' cohort if provided
#'
get_conditions <- function(provided_cohort,
                           condition_codeset,
                           condition_tbl = cdm_tbl("condition_occurrence")) {
  condition_vctr <-
    condition_codeset %>% distinct(concept_id) %>% pull()
  
  if (missing(provided_cohort)) {
    cohort_birth_dates <- cdm_tbl("person") %>%
      select(person_id, birth_date)
  }
  else {
    cohort_birth_dates <- cdm_tbl("person") %>%
      inner_join(select(provided_cohort, person_id), by = "person_id") %>%
      select(person_id, birth_date)
  }
  
  conditions <- condition_tbl %>%
    inner_join(select(cohort_birth_dates, person_id, birth_date), by = "person_id") %>%
    filter(condition_concept_id %in% condition_vctr) %>%
    mutate(condition_age_days = condition_start_date - birth_date)
  
  return(conditions)
}

#' This file contains functions to identify cohorts in a study.  Its
#' contents, as well as the contents of any R files whose name begins
#' with "cohort_", will be automatically sourced when the request is
#' executed.
#'
#' For simpler requests, it will often be possible to do all cohort
#' manipulation in functions within this file.  For more complex
#' requests, it may be more readable to separate different cohorts or
#' operations on cohorts into logical groups in different files.

#' Get eligible condition info for patients with codes in the
#' inclusion codeset and no codes in the exclusion codeset
#'
#' @param incl_codeset_name
#' @param excl_codeset_name
#'
#' @return Table of condition info for patients with codes in the
#' inclusion codeset and no codes in the exclusion codeset
get_elig_conds_for_i_e_pats <- function(incl_codeset_name,
                                        excl_codeset_name,
                                        cond_tbl=cdm_tbl('condition_occurrence')) {
  pats_incl <- cond_tbl %>%
    inner_join(load_codeset(incl_codeset_name),
               by = c("condition_concept_id" = "concept_id")) %>%
    distinct(
      condition_occurrence_id,
      person_id,
      site,
      visit_occurrence_id,
      condition_concept_id,
      condition_concept_name,
      condition_type_concept_id,
      condition_start_date,
      condition_source_value
    ) %>%
    compute_new()
  
  pats_excl <- cond_tbl %>%
    inner_join(load_codeset(excl_codeset_name),
               by = c("condition_concept_id" = "concept_id")) %>%
    distinct(person_id) %>%
    compute_new()
  
  elig_conds_for_i_e_pats <- pats_incl %>%
    anti_join(pats_excl, by = "person_id") %>%
    compute_new()
  
  return(elig_conds_for_i_e_pats)
  
}


#'  Get condition table with flags for patients with inclusion conditions who do not have
#'  conditions of exclusion AND who have eligible visits (more than one visit with a nephrology
#'  or rheumotology provider)
#'
#' @param elig_conds_for_i_e_pats Patients with any of the inclusion criteria and none of the exclusion criteria
#' @param incl_provider_spec_name Name of provider specialty codeset
#' @param incl_visit_name Name of visit type codeset
#'
#' @return Get condition table with flags for patients with inclusion conditions who do not have
#'  conditions of exclusion AND who have eligible visits (more than one visit with a nephrology
#'  or rheumotology provider)
get_elig_conds_for_i_e_pats_w_elig_visits <-
  function(incl_spec_codeset,
           incl_visit_codeset,
           elig_conds_for_i_e_pats) {
    
    i_e_pats <- elig_conds_for_i_e_pats %>% 
      distinct(person_id) %>% 
      compute_new()
    
    elig_visits <-
      get_elig_visits_for_cohort(
        incl_spec_codeset = incl_spec_codeset,
        incl_visit_codeset = incl_visit_codeset,
        cohort = i_e_pats
      ) %>% 
      compute_new()
    
    elig_visits_pats <- elig_visits %>% 
      distinct(person_id) %>% 
      compute_new()
    
    elig_conds_for_i_e_pats_w_elig_visits <-
      elig_conds_for_i_e_pats %>%
      inner_join(select(elig_visits, person_id),
                 by = "person_id") %>%
      distinct(
        condition_occurrence_id,
        person_id,
        site,
        visit_occurrence_id,
        condition_concept_id,
        condition_concept_name,
        condition_type_concept_id,
        condition_start_date,
        condition_source_value
      ) %>%
      compute_new()
    
    return(elig_conds_for_i_e_pats_w_elig_visits)
    
  }



#' Get SLE cohort
#'
#' 2 or more in-person visits with a nephrology or rheumatology provider or care
#' site, AND
#' 60 or more days follow-up, AND
#' No occurrence of neonatal lupus erythematosus, AND
#' 1 or more hydroxychloroquine drug exposure at any time, AND
#' 3 or more SLE inclusion codes associated with any visit type each separated 30
#' or more days, OR
#' 1 or more SLE inclusion code associated with any visit type at any time AND 1
#' or more kidney biopsy procedure code at any time
#'
#' @param elig_conds_sle_i_e_pats_w_elig_visits SLE condition rows for patients
#' who meet other criteria
#' @param drug_codeset Drug codeset
#'
#' @return Patients who meet SLE cohort criteria
#' 
get_sle_cohort <- function(elig_conds_sle_i_e_pats_w_elig_visits,
                           drug_codeset) {
  sle_alg_b <- get_alg_b(elig_conds_sle_i_e_pats_w_elig_visits) %>%
    compute_new()
  
  sle_i_e_pats_w_elig_visits <- elig_conds_sle_i_e_pats_w_elig_visits %>% 
    distinct(person_id)
  
  kidney_biopsy <-
    get_patients_with_kidney_biopsy(sle_i_e_pats_w_elig_visits) %>%
    get_patients_w_60_or_more_days_followup() %>% 
    compute_new()
  
  sle_cohort <- sle_alg_b %>%
    dplyr::union(kidney_biopsy) %>%
    compute_new() %>%
    get_patients_w_drug_exposure(drug_codeset) %>%
    add_site() %>% 
    compute_new()
  
}


#' Get kidney transplant procedures and conditions, restricted to a cohort
#' if provided, from condition_occurrence and procedure_occurrence CDM tables
#'
#' @param cohort Cohort
#' @param kidney_transplant_codeset Kidney transplant codeset
#' @param age_ub_years Age upper bound
#' @param min_date_cutoff Date before which visits are excluded
#' @param max_date_cutoff Date after which visits are excluded
#'
#' @return Table with the following fields: person_id, site, concept_id,
#' source_value, concept_name, record_type (procedure/condition), and date
#' for kidney transplant procedures and conditions
#'
get_kidney_transplant_broad <- function(cohort,
                                        kidney_transplant_codeset = 
                                          load_codeset('kidney_transplant_broad','icccc'),
                                        age_ub_years = 30L,
                                        min_date_cutoff = as.Date("2009-01-01"),
                                        max_date_cutoff = as.Date("2021-01-01")) {
  
  kidney_transplant_proc <- get_procedures(provided_cohort = cohort,
                                           procedure_codeset = kidney_transplant_codeset) %>%
    mutate(table_name = as.character("procedure"),
           approach = as.character("proc")) %>%
    select(
      person_id,
      site,
      occurrence_id = procedure_occurrence_id,
      concept_id = procedure_concept_id,
      source_value = procedure_source_value,
      concept_name = procedure_concept_name,
      table_name,
      approach,
      transplant_date = procedure_date,
      age_in_days = procedure_age_days
    )
  
  kidney_transplant_cond <- get_conditions(provided_cohort = cohort,
                                           condition_codeset = kidney_transplant_codeset) %>%
    mutate(table_name = as.character("condition"),
           approach = as.character("cond")) %>%
    select(
      person_id,
      site,
      occurrence_id = condition_occurrence_id,
      concept_id = condition_concept_id,
      source_value = condition_source_value,
      concept_name = condition_concept_name,
      table_name,
      approach,
      transplant_date = condition_start_date,
      age_in_days = condition_age_days
    )
  
  kidney_transplant_occurrences <- kidney_transplant_cond %>%
    dplyr::union(kidney_transplant_proc) %>% 
    filter(age_in_days < 365.25 * age_ub_years,
           transplant_date >= min_date_cutoff,
           transplant_date < max_date_cutoff)
  
  return(kidney_transplant_occurrences)
  
}



#' function to compute follow up time
#' 
#' @param condition_tbl conditions to get date of entry
#' @param visit_tbl viisits to derive last visit date
#' @param transplant_tbl tbl which contains transplant_date
#' @param cohort cohort of interest
#' 
#' @return tbl with the following columns:
#' 
#' person_id | sitet | cohort_entry | cohort_exit | follow_up_days | follow_up_yrs
#' 

generate_followup <- function(condition_tbl,
                              visit_tbl,
                              transplant_tbl,
                              cohort,
                              nephrotic_codeset=
                                load_codeset('glomerular_disease','iccciciiii') %>% 
                                filter(codeset_category_code == 1)) {
  
  nephrotic_first <- 
    cohort %>%
    select(person_id) %>% distinct %>%
    inner_join(
      condition_tbl,
      by='person_id'
    ) %>%
    inner_join(
      select(
        nephrotic_codeset,
        concept_id
      ), by = 
        c('condition_concept_id'='concept_id')
    ) %>% 
    group_by(person_id,site) %>%
    summarise(
      cohort_entry=min(condition_start_date)
    ) %>% ungroup() %>% compute_new(temporary=TRUE)
  
  transplant_dt <- 
    transplant_tbl %>%
    group_by(
      person_id,
      site
    ) %>% 
    summarise(
      cohort_exit=min(transplant_date)
    ) %>% ungroup() %>% compute_new(temporary=TRUE)
  
  last <- 
    cohort %>%
    anti_join(
      transplant_dt,
      by='person_id'
    ) %>%
    inner_join(
      select(
        visit_tbl,
        person_id,
        visit_start_date,
        visit_concept_id,
        site
      ), by='person_id'
    ) %>% 
    filter(
      visit_concept_id %in% c(9201L,
                              9202L,
                              9203L,
                              2000000048L,
                              2000000088L,
                              2000000469L)
    ) %>%
    group_by(person_id,site) %>%
    summarise(
      cohort_exit=max(visit_start_date)
    ) %>% ungroup() %>% compute_new(temporary=TRUE)
  
  cohort_exit <- 
    dplyr::union(transplant_dt,
                 last) 
  
  flags <- 
    cohort %>%
    select(person_id) %>%
    left_join(nephrotic_first, by = "person_id") %>%
    left_join(cohort_exit, by = c("person_id", "site")) %>%
    mutate(follow_up_days = cohort_exit - cohort_entry) %>%
    mutate(follow_up_yrs = round(follow_up_days/365.25, 2)) %>%
    mutate(cohort_entry_year=sql("extract(year from cohort_entry)")) %>%
    mutate(cohort_exit_year=sql("extract(year from cohort_exit)"))
  
}

