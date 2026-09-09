# ==============================================================================
# INPI PATENT DATA PIPELINE - PRODUCTION VERSION (2025-02-03)
# ==============================================================================
# CRITICAL UPDATES:
# - FRAMD XML parsing corrected (kind attribute from root node)
# - Citation ID normalization fixed (remove ALL whitespace)
# - Inventor deduplication per year
# - Parquet schema enforcement
# ==============================================================================

options(scipen = 999)
library(arrow)
library(tidyverse)
library(xml2)
library(furrr)
library(future)
library(data.table)

# Configuration
# NOTE: replace with your own object storage bucket/paths before running.
CONFIG <- list(
  bucket = "<your-bucket-name>",
  s3_input = "data INPI - Notices bibliographiques.zip",
  s3_output = "RA INPI/INPI_FINAL_0302",
  work_dir = "/home/onyxia/work",
  n_workers = min(20, parallel::detectCores() - 2),
  batch_size = 100
)

plan(multisession, workers = CONFIG$n_workers)

# Helper functions
clean_text <- function(x) {
  if(is.na(x)) return(NA_character_)
  str_trim(str_replace_all(str_replace_all(x, ";", ","), "[\r\n]+", " "))
}

get_text <- function(doc, xpaths) {
  for(xp in xpaths) {
    node <- xml_find_first(doc, xp)
    if(!is.na(node)) {
      text <- xml_text(node, trim = TRUE)
      if(text != "") return(text)
    }
  }
  NA_character_
}

get_all_text <- function(doc, xpaths) {
  vals <- character(0)
  for(xp in xpaths) {
    nodes <- xml_find_all(doc, xp)
    if(length(nodes) > 0) vals <- c(vals, xml_text(nodes, trim = TRUE))
  }
  vals <- unique(vals[vals != ""])
  if(length(vals) > 0) paste(vals, collapse = "; ") else NA_character_
}

extract_parties <- function(doc, xpaths) {
  nodes <- NULL
  for(xp in xpaths) {
    nodes <- xml_find_all(doc, xp)
    if(length(nodes) > 0) break
  }
  if(length(nodes) == 0) return(list(names = NA, locs = NA, ids = NA))
  info <- map(nodes, function(n) {
    org <- xml_find_first(n, ".//addressbook/orgname")
    ln <- xml_find_first(n, ".//addressbook/last-name")
    fn <- xml_find_first(n, ".//addressbook/first-name")
    name <- if(!is.na(org)) xml_text(org, trim = TRUE)
      else if(!is.na(ln)) paste(na.omit(c(xml_text(ln, trim = TRUE),
        if(!is.na(fn)) xml_text(fn, trim = TRUE) else NULL)),
        collapse = " ")
      else NA_character_
    city <- xml_find_first(n, ".//addressbook/address/city")
    country <- xml_find_first(n, ".//addressbook/address/country")
    loc <- paste(na.omit(c(if(!is.na(city)) xml_text(city, trim = TRUE) else NULL,
      if(!is.na(country)) xml_text(country, trim = TRUE) else NULL)),
      collapse = ", ")
    id_node <- xml_find_first(n, ".//addressbook/iid | .//addressbook/org-id | .//addressbook/registered-number")
    id_val <- if(!is.na(id_node)) xml_text(id_node, trim = TRUE) else xml_attr(n, "app-type")
    list(name = name, loc = if(loc == "") NA_character_ else loc, id = id_val)
  })
  names_str <- paste(na.omit(map_chr(info, "name")), collapse = "; ")
  locs_str <- paste(na.omit(map_chr(info, "loc")), collapse = "; ")
  ids_str <- paste(na.omit(map_chr(info, "id")), collapse = "; ")
  list(names = if(names_str == "") NA_character_ else names_str,
    locs = if(locs_str == "") NA_character_ else locs_str,
    ids = if(ids_str == "") NA_character_ else ids_str)
}

# CORRECTED: FRAMD XML parser
parse_xml <- function(file) {
  tryCatch({
    doc <- read_xml(file)
    xml_ns_strip(doc)
    root <- xml_find_first(doc, ".")
    kind_root <- xml_attr(root, "kind")
    status_attr <- xml_attr(root, "status")
    pub_id <- get_text(doc, c(".//fr-publication-reference//doc-number",
      ".//publication-reference//doc-number"))
    if(is.na(pub_id)) return(NULL)

    # CRITICAL: Use kind from root attribute if available (FRAMD files)
    kind <- if(!is.na(kind_root) && kind_root != "") kind_root
      else get_text(doc, c(".//fr-publication-reference//kind",
        ".//publication-reference//kind"))

    prio_nodes <- xml_find_all(doc, ".//fr-priority-claim | .//priority-claim")
    if(length(prio_nodes) > 0) {
      prio_country <- paste(na.omit(map_chr(prio_nodes, ~{
        cn <- xml_find_first(., "country")
        if(!is.na(cn)) xml_text(cn, trim = TRUE) else NA_character_
      })), collapse = "; ")
      prio_date <- paste(na.omit(map_chr(prio_nodes, ~{
        dt <- xml_find_first(., "date")
        if(!is.na(dt)) xml_text(dt, trim = TRUE) else NA_character_
      })), collapse = "; ")
    } else {
      prio_country <- NA_character_
      prio_date <- NA_character_
    }

    app <- extract_parties(doc, c(".//fr-applicants/fr-applicant", ".//applicants/applicant"))
    own <- extract_parties(doc, c(".//parties/fr-owners/fr-owner", ".//parties/assignees/assignee"))
    inv <- extract_parties(doc, c(".//fr-inventors/fr-inventor", ".//inventors/inventor"))

    firm_names <- if(!is.na(own$names) && own$names != "") own$names else app$names
    firm_locs <- if(!is.na(own$locs) && own$locs != "") own$locs else app$locs
    firm_ids <- if(!is.na(own$ids) && own$ids != "") own$ids else app$ids

    cit_nodes <- xml_find_all(doc, ".//references-cited/citation/patcit/document-id | .//patent-citations//citation/patcit/document-id")
    refs <- if(length(cit_nodes) > 0) {
      map_chr(cit_nodes, function(n) {
        country <- xml_find_first(n, "country")
        docnum <- xml_find_first(n, "doc-number")
        c_val <- if(!is.na(country)) xml_text(country, trim = TRUE) else NA_character_
        d_val <- if(!is.na(docnum)) xml_text(docnum, trim = TRUE) else NA_character_
        # CRITICAL: Remove whitespace from doc-number
        if(!is.na(d_val)) d_val <- str_replace_all(d_val, "\\s+", "")
        paste(na.omit(c(c_val, d_val)), collapse = "")
      }) %>% unique() %>% paste(collapse = "; ")
    } else NA_character_

    data.frame(
      id = pub_id, kind = kind, status = status_attr,
      date_pub = get_text(doc, c(".//fr-publication-reference//date", ".//publication-reference//date")),
      date_app = get_text(doc, c(".//fr-application-reference//date", ".//application-reference//date")),
      date_grant = get_text(doc, c(".//fr-date-granted/date")),
      date_priority = prio_date, priority_country = prio_country,
      title = clean_text(get_text(doc, ".//invention-title")),
      firm_names = clean_text(firm_names),
      firm_locations = clean_text(firm_locs),
      firm_ids = firm_ids,
      inventor_names = clean_text(inv$names),
      inventor_locations = clean_text(inv$locs),
      ipc_codes = get_all_text(doc, ".//classification-ipcr//text"),
      cpc_codes = get_all_text(doc, c(".//classification-cpc//text", ".//classification-cpc//cpc-symbol")),
      references_list = refs,
      source_file = basename(file),
      stringsAsFactors = FALSE
    )
  }, error = function(e) NULL)
}

# PHASE 1: Extraction
extract_data <- function() {
  cat("\n[PHASE 1] Extraction\n")
  zip_path <- file.path(CONFIG$work_dir, "master.zip")
  if(!file.exists(zip_path)) {
    system(paste("mc cp",
      shQuote(paste0("s3/", CONFIG$bucket, "/", CONFIG$s3_input)),
      shQuote(zip_path)))
  }
  files_raw <- system(paste("unzip -l", shQuote(zip_path)), intern = TRUE)
  files_list <- str_trim(str_replace(files_raw, "^.*\\d{2}:\\d{2}\\s+", ""))
  targets <- unique(files_list[str_detect(files_list, "FRNEW|FRAMD|FRBREVETS|Backlog")])
  cat(sprintf("Target files: %d\n", length(targets)))

  cp_file <- file.path(CONFIG$work_dir, "checkpoints/done.json")
  dir.create(dirname(cp_file), showWarnings = FALSE, recursive = TRUE)
  done <- if(file.exists(cp_file)) fromJSON(cp_file)$done else character(0)
  todo <- setdiff(targets, done)

  if(length(todo) == 0) {
    cat("Extraction complete\n")
    return()
  }

  cat(sprintf("Processing: %d\n", length(todo)))
  work <- file.path(CONFIG$work_dir, "temp")
  out_dir <- file.path(CONFIG$work_dir, "parquet_raw")
  if(dir.exists(work)) unlink(work, recursive = TRUE)
  dir.create(work)
  dir.create(out_dir, showWarnings = FALSE)

  pb <- txtProgressBar(0, length(todo), style = 3)
  batch_done <- character(0)

  for(i in seq_along(todo)) {
    f <- todo[i]
    system(paste("unzip -jo", shQuote(zip_path), shQuote(f), "-d", shQuote(work)),
      ignore.stdout = TRUE)
    local_f <- file.path(work, basename(f))
    if(file.exists(local_f)) {
      xmls <- if(str_detect(local_f, "\\.zip$")) {
        nested <- file.path(work, "nested")
        dir.create(nested, showWarnings = FALSE)
        system(paste("unzip -q", shQuote(local_f), "-d", shQuote(nested)), ignore.stdout = TRUE)
        # Exclude metadata files
        all_xmls <- list.files(nested, "\\.xml$", full.names = TRUE, recursive = TRUE)
        all_xmls[!str_detect(basename(all_xmls), "^(index|volumeid|contents|TOC|package)")]
      } else {
        local_f
      }
      if(length(xmls) > 0) {
        res <- map_dfr(xmls, parse_xml)
        if(nrow(res) > 0) {
          write_parquet(res, file.path(out_dir, paste0(gsub("[/\\\\]", "_", f), ".parquet")))
        }
      }
      unlink(list.files(work, full.names = TRUE), recursive = TRUE)
    }
    batch_done <- c(batch_done, f)
    if(i %% CONFIG$batch_size == 0 || i == length(todo)) {
      temp_cp <- paste0(cp_file, ".tmp")
      write_json(list(done = unique(c(done, batch_done))), temp_cp)
      file.rename(temp_cp, cp_file)
      batch_done <- character(0)
      gc(verbose = FALSE)
    }
    if(i %% 10 == 0) gc(verbose = FALSE)
    setTxtProgressBar(pb, i)
  }
  close(pb)
  unlink(work, recursive = TRUE)
}

# ==============================================================================
# PHASE 2: Consolidation
# ==============================================================================
consolidate_data <- function() {
  cat("\n[PHASE 2] Consolidation\n")
  schema_def <- schema(
    id = string(), kind = string(), status = string(), date_pub = string(),
    date_app = string(), date_grant = string(), date_priority = string(),
    priority_country = string(), title = string(), firm_names = string(),
    firm_locations = string(), firm_ids = string(), inventor_names = string(),
    inventor_locations = string(), ipc_codes = string(), cpc_codes = string(),
    references_list = string(), source_file = string()
  )
  ds <- open_dataset(file.path(CONFIG$work_dir, "parquet_raw"), schema = schema_def)
  df <- ds %>% collect() %>% as.data.table()

  cat(sprintf("Raw rows: %s\n", format(nrow(df), big.mark = ",")))
  if(nrow(df) == 0) return(df)

  df[, `:=`(
    pub_date = as.Date(date_pub, format = "%Y%m%d"),
    app_date = as.Date(date_app, format = "%Y%m%d"),
    grant_date = as.Date(date_grant, format = "%Y%m%d")
  )]

  df <- df[!is.na(pub_date)]
  df[, pub_year := year(pub_date)]

  is_fr <- function(x) str_detect(x, regex("\\bFR\\b|France", ignore_case = TRUE))
  cat("French filter\n")
  df <- df[!is.na(pub_year) & (
    (!is.na(priority_country) & is_fr(priority_country)) |
    (!is.na(firm_locations) & is_fr(firm_locations)) |
    (!is.na(inventor_locations) & is_fr(inventor_locations))
  )]
  cat(sprintf("Filtered: %s\n", format(nrow(df), big.mark = ",")))

  cat("Deduplication\n")
  df_final <- df %>%
    group_by(id, kind) %>%
    mutate(score = str_length(paste0(firm_names, references_list))) %>%
    arrange(desc(score)) %>%
    slice(1) %>%
    ungroup() %>%
    as.data.table()

  cat("\nKIND distribution:\n")
  print(table(df_final$kind, useNA = "ifany"))

  return(df_final)
}

# ==============================================================================
# PHASE 3: Citations
# ==============================================================================
calculate_citations <- function(df) {
  cat("\n[PHASE 3] Citations\n")
  if(nrow(df) == 0) return(df)

  edges <- df[references_list != "",
    .(cited_raw = unlist(str_split(references_list, "; "))),
    by = .(citing_id = id, citing_date = pub_date)]
  cat(sprintf("Raw edges: %s\n", format(nrow(edges), big.mark = ",")))

  normalize_id <- function(x) {
    x <- toupper(str_trim(x))
    x <- str_replace(x, "^[A-Z]{2}", "")
    x <- str_replace_all(x, "\\s+", "")
    x
  }

  edges[, `:=`(
    cited_id = normalize_id(cited_raw),
    citing_id_norm = normalize_id(citing_id)
  )]
  edges <- edges[citing_id_norm != cited_id]
  edges <- unique(edges, by = c("citing_id_norm", "cited_id"))
  cat(sprintf("After cleanup: %s\n", format(nrow(edges), big.mark = ",")))

  patent_dates <- df[, .(
    id_norm = normalize_id(id),
    pub_date = pub_date
  )]
  patent_dates <- patent_dates %>%
    group_by(id_norm) %>%
    arrange(pub_date) %>%
    slice(1) %>%
    ungroup() %>%
    as.data.table()

  edges <- merge(edges, patent_dates,
    by.x = "cited_id", by.y = "id_norm",
    all.x = FALSE)
  setnames(edges, "pub_date", "cited_date")
  edges[, time_lag := as.numeric(citing_date - cited_date) / 365.25]

  cit_counts <- edges[time_lag >= 0 & time_lag <= 5,
    .(citations_5y = .N),
    by = cited_id]

  df[, id_norm := normalize_id(id)]
  final <- merge(df, cit_counts,
    by.x = "id_norm", by.y = "cited_id",
    all.x = TRUE)
  final[is.na(citations_5y), citations_5y := 0]
  final[, id_norm := NULL]

  if("firm_ids" %in% names(final)) {
    final[, firm_ids := str_replace_all(firm_ids, "\\s+", "")]
  }

  cat(sprintf("\nCitation stats:\n"))
  cat(sprintf(" Cited: %s (%.1f%%)\n",
    format(sum(final$citations_5y > 0), big.mark = ","),
    100 * mean(final$citations_5y > 0)))
  cat(sprintf(" Mean: %.2f | Median: %.0f | Max: %d\n",
    mean(final$citations_5y),
    median(final$citations_5y),
    max(final$citations_5y)))

  return(final)
}

# ==============================================================================
# PHASE 4 & 5: Export
# ==============================================================================
export_data <- function(df) {
  cat("\n[EXPORT] Saving\n")
  if(nrow(df) == 0) {
    cat("No data to export.\n")
    return()
  }

  df_main <- df[pub_year >= 1970 & pub_year <= 2025]
  local_pat <- file.path(CONFIG$work_dir, "dataset_diviso_anni_corrected")
  if(dir.exists(local_pat)) unlink(local_pat, recursive = TRUE)
  dir.create(local_pat)

  years <- sort(unique(df_main$pub_year))
  if(length(years) > 0) {
    pb <- txtProgressBar(0, length(years), style = 3)
    for(i in seq_along(years)) {
      y <- years[i]
      fwrite(df_main[pub_year == y],
        file.path(local_pat, sprintf("INPI_patents_%d.csv", y)),
        sep = ",", na = "", quote = TRUE)
      setTxtProgressBar(pb, i)
    }
    close(pb)
    system(paste("mc cp --recursive", shQuote(local_pat),
      shQuote(paste0("s3/", CONFIG$bucket, "/", CONFIG$s3_output, "/"))))
  }

  cat("\nInventors dataset\n")
  df_inv_base <- df[pub_year >= 1970 & pub_year <= 2025 &
    !is.na(inventor_names) & inventor_names != ""]

  if(nrow(df_inv_base) > 0) {
    df_inv_base[, kind_priority := fcase(
      kind == "B1", 1, kind == "B3", 2, kind == "A1", 3, kind == "A3", 4, default = 5
    )]
    df_inv_dedup <- df_inv_base %>%
      group_by(id, pub_year) %>%
      arrange(kind_priority) %>%
      slice(1) %>%
      ungroup() %>%
      as.data.table()

    df_inv <- df_inv_dedup[, .(id, pub_year, inventor_names, inventor_locations)]
    local_inv <- file.path(CONFIG$work_dir, "dataset_inventors_unique")
    if(dir.exists(local_inv)) unlink(local_inv, recursive = TRUE)
    dir.create(local_inv)

    align_names <- function(n, l) {
      ns <- str_trim(unlist(str_split(n, "; ")))
      ls <- if(is.na(l) || l == "") rep(NA_character_, length(ns))
        else str_trim(unlist(str_split(l, "; ")))
      if(length(ls) == 1 && length(ns) > 1) ls <- rep(ls, length(ns))
      else if(length(ls) < length(ns)) ls <- c(ls, rep(NA_character_, length(ns) - length(ls)))
      else if(length(ls) > length(ns)) ls <- ls[1:length(ns)]
      list(name = ns, loc = ls)
    }

    years_inv <- sort(unique(df_inv$pub_year))
    pb <- txtProgressBar(0, length(years_inv), style = 3)
    for(i in seq_along(years_inv)) {
      y <- years_inv[i]
      df_y <- df_inv[pub_year == y]
      pl <- pmap(list(df_y$inventor_names, df_y$inventor_locations), align_names)
      df_long <- data.table(
        id = rep(df_y$id, times = map_int(pl, ~length(.x$name))),
        pub_year = y,
        inventor_name = unlist(map(pl, "name")),
        inventor_location = unlist(map(pl, "loc"))
      )
      df_long_unique <- df_long %>%
        group_by(inventor_name, pub_year) %>%
        slice(1) %>%
        ungroup() %>%
        as.data.table()
      fwrite(df_long_unique,
        file.path(local_inv, sprintf("INPI_inventors_%d.csv", y)),
        sep = ",", na = "", quote = TRUE)
      setTxtProgressBar(pb, i)
    }
    close(pb)

    cat("\nInventor stats:\n")
    all_inv <- rbindlist(lapply(years_inv, function(y) {
      fread(file.path(local_inv, sprintf("INPI_inventors_%d.csv", y)))
    }))
    cat(sprintf(" Total (name, year): %s\n", format(nrow(all_inv), big.mark = ",")))
    cat(sprintf(" Unique names: %s\n", format(uniqueN(all_inv$inventor_name), big.mark = ",")))

    system(paste("mc cp --recursive", shQuote(local_inv),
      shQuote(paste0("s3/", CONFIG$bucket, "/", CONFIG$s3_output, "/"))))
  }

  cat("\nPIPELINE COMPLETE\n")
  cat(sprintf("Output: s3/%s/%s/\n", CONFIG$bucket, CONFIG$s3_output))
  cat(sprintf("Patents: %s\n", format(nrow(df), big.mark = ",")))
  if (nrow(df) > 0) {
    cat(sprintf("Years: %d-%d\n", min(df$pub_year, na.rm = TRUE), max(df$pub_year, na.rm = TRUE)))
  }
}

# ==============================================================================
# EXECUTE
# ==============================================================================
cat("\n")
cat(strrep("=", 80), "\n", sep = "")
cat("INPI PIPELINE\n")
cat(strrep("=", 80), "\n", sep = "")

# Skipped extract_data() if already done
# extract_data()
df_cons <- consolidate_data()
df_final <- calculate_citations(df_cons)
export_data(df_final)

cat("\nDONE\n")
