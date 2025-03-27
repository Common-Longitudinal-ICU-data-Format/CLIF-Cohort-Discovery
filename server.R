library(tableone)
library(shinyjs)

# Define Server
server <- function(input, output, session) {

  flog.info("Server function started.")

  # Connect to DuckDB
  db_connection <- tryCatch({
    dbConnect(duckdb::duckdb(), dbdir = "cohort.duckdb")
  }, error = function(e) {
    flog.error("Error connecting to DuckDB: %s", e$message)
    NULL  # Prevent crash
  })

  uploaded_data <- reactiveVal(list())
  cohort_criteria_count <- reactiveVal(1)
  inclusion_criteria_count <- reactiveVal(1)
  exclusion_criteria_count <- reactiveVal(1)

  # Track if files are uploaded
  output$filesUploaded <- reactive({
    !is.null(input$main) && nrow(input$main) > 0
  })
  outputOptions(output, "filesUploaded", suspendWhenHidden = FALSE)

  # Processed data flag
  processed_data <- reactiveVal(FALSE)
  output$dataReady <- reactive({
    return(processed_data())
  })
  outputOptions(output, "dataReady", suspendWhenHidden = FALSE)

  # Handle File Uploads
  observeEvent(input$main, {
    flog.info("File upload triggered.")
    req(input$main)
    temp_data <- list()
    for (i in seq_along(input$main$name)) {
      tryCatch({
        flog.info("Processing file: %s", input$main$name[i])
        if (grepl("\\.parquet$", input$main$name[i])) {
          temp_data[[input$main$name[i]]] <- input$main$datapath[i]
        } else if (grepl("\\.csv$", input$main$name[i])) {
          temp_data[[input$main$name[i]]] <- input$main$datapath[i]
        }
      }, error = function(e) {
        flog.error("Error reading file %s: %s", input$main$name[i], e$message)
      })
    }
    if (length(temp_data) > 0) {
      flog.info("Files uploaded: %s", toString(names(temp_data)))
      uploaded_data(temp_data)
      result <- register_all_tables(uploaded_data())
      processed_data(result)
      if (result) {
        flog.info("File processing successful.")
        showNotification("Files processed successfully!", type = "message")
      } else {
        flog.error("File processing failed.")
        showNotification("Error processing files. Please try again.", type = "error")
      }
    } else {
      flog.error("No valid data files were uploaded.")
    }
  })

  # Function to register all tables in DuckDB
  register_all_tables <- function(uploaded_data) {
    flog.info("Registering tables in DuckDB...")
    success_count <- 0
    for (file_name in names(uploaded_data)) {
      tryCatch({
        table_name <- gsub("\\.(csv|parquet)$", "", file_name)
        file_path <- uploaded_data[[file_name]]
        flog.info("Registering table: %s from file: %s", table_name, file_name)
        if (!table_name %in% dbListTables(db_connection)) {
          dbExecute(db_connection, paste0("CREATE OR REPLACE TABLE ", table_name, 
                                          " AS SELECT * FROM read_parquet('", file_path, "')"))
          flog.info("Table %s registered successfully.", table_name)
          success_count <- success_count + 1
        } else {
          flog.info("Table %s already registered, skipping.", table_name)
          success_count <- success_count + 1
        }
      }, error = function(e) {
        flog.error("Error registering table %s: %s", file_name, e$message)
      })
    }
    flog.info("Total tables processed: %s", success_count)
    return(success_count > 0)
  }

  # Initialize inputs to NULL
  observe({
    flog.info("Initializing UI inputs to NULL.")
    updateSelectizeInput(session, "cohort_entry_events_1", selected = NULL)
    updateSelectizeInput(session, "cohort_column_selection_1", selected = NULL)
  })

  # Generate table choices dynamically
  table_choices <- reactive({
    files <- names(uploaded_data())
    cleaned_names <- gsub("\\.(csv|parquet)$", "", files, ignore.case = TRUE)
    flog.info("Available tables: %s", toString(cleaned_names))
    return(cleaned_names)
  })

  ### ---------------------- UI for Cohort Entry Selection ---------------------- ###
  output$cohort_entry_ui <- renderUI({
    flog.info("Rendering Cohort Entry UI.")
    num_criteria <- cohort_criteria_count()
    criteria_ui <- lapply(1:num_criteria, function(i) {
      fluidRow(
        tags$h5(paste("Cohort Entry Event", i)),
        selectizeInput(paste0("cohort_entry_events_", i), "Select Table",
                       choices = c("None" = "", table_choices()), selected = "", multiple = FALSE),
        selectizeInput(paste0("cohort_column_selection_", i), "Select Column",
                       choices = c("None" = ""), selected = "", multiple = FALSE),
        uiOutput(paste0("cohort_dynamic_filter_ui_", i)),
        fluidRow(
          column(6,
                 selectInput(paste0("time_limit_option_", i), "Limit Records",
                             choices = c("All Records" = "all", 
                                         "Earliest Record" = "earliest",
                                         "Latest Record" = "latest"),
                             selected = "all")
          ),
          column(6,
                 uiOutput(paste0("timestamp_column_ui_", i))
          )
        )
      )
    })
    do.call(tagList, criteria_ui)
  })

  observe({
    flog.info("Rendering timestamp column UI for Cohort Entry.")
    num_criteria <- cohort_criteria_count()
    for (i in 1:num_criteria) {
      local({
        current_i <- i
        output[[paste0("timestamp_column_ui_", current_i)]] <- renderUI({
          req(input[[paste0("time_limit_option_", current_i)]])
          time_option <- input[[paste0("time_limit_option_", current_i)]]
          selected_table <- input[[paste0("cohort_entry_events_", current_i)]]
          flog.info("Time limit option for entry %s: %s, table: %s", current_i, time_option, selected_table)
          if (time_option != "all" && !is.null(selected_table) && selected_table != "") {
            timestamp_columns <- get_timestamp_columns(selected_table)
            if (length(timestamp_columns) > 0) {
              selectizeInput(
                paste0("timestamp_column_", current_i),
                "Order By",
                choices = timestamp_columns,
                selected = timestamp_columns[1]
              )
            } else {
              div(class = "alert alert-warning", "No timestamp columns found in this table")
            }
          }
        })
      })
    }
  })

  get_timestamp_columns <- function(selected_table) {
    flog.info("Getting timestamp columns for table: %s", selected_table)
    tryCatch({
      type_query <- paste0("PRAGMA table_info(", selected_table, ")")
      column_info <- dbGetQuery(db_connection, type_query)
      timestamp_cols <- column_info$name[
        grepl("DATE|TIMESTAMP|TIME|_DT$|_DATE$", column_info$type, ignore.case = TRUE) | 
          grepl("DATE|TIME|_DT$|_DATE$", column_info$name, ignore.case = TRUE)
      ]
      flog.info("Timestamp columns for %s: %s", selected_table, toString(timestamp_cols))
      return(timestamp_cols)
    }, error = function(e) {
      flog.error("Error getting timestamp columns for %s: %s", selected_table, e$message)
      return(character(0))
    })
  }

  ### ---------------------- UI for Inclusion & Exclusion Criteria ---------------------- ###
  output$inclusion_criteria_ui <- renderUI({
    flog.info("Rendering Inclusion Criteria UI.")
    num_criteria <- inclusion_criteria_count()
    criteria_ui <- lapply(1:num_criteria, function(i) {
      fluidRow(
        tags$h5(paste("Inclusion Criteria", i)),
        selectizeInput(paste0("inclusion_criteria_events_", i), "Select Table",
                       choices = c("None" = "", table_choices()), selected = "", multiple = FALSE),
        selectizeInput(paste0("inclusion_column_selection_", i), "Select Column",
                       choices = c("None" = ""), selected = "", multiple = FALSE),
        uiOutput(paste0("inclusion_dynamic_filter_ui_", i)),
        # New time limit UI for inclusion criteria:
        fluidRow(
          column(6,
                 selectInput(paste0("time_limit_option_inclusion_", i), "Limit Records",
                             choices = c("All Records" = "all", 
                                         "Earliest Record" = "earliest",
                                         "Latest Record" = "latest"),
                             selected = "all")
          ),
          column(6,
                 uiOutput(paste0("timestamp_column_ui_inclusion_", i))
          )
        )
      )
    })
    do.call(tagList, criteria_ui)
  })

  # Render timestamp column UI for inclusion criteria
  observe({
    flog.info("Rendering timestamp column UI for Inclusion Criteria.")
    num_inclusion <- inclusion_criteria_count()
    for (i in 1:num_inclusion) {
      local({
        current_i <- i
        output[[paste0("timestamp_column_ui_inclusion_", current_i)]] <- renderUI({
          req(input[[paste0("time_limit_option_inclusion_", current_i)]])
          time_option <- input[[paste0("time_limit_option_inclusion_", current_i)]]
          selected_table <- input[[paste0("inclusion_criteria_events_", current_i)]]
          flog.info("Time limit option for inclusion %s: %s, table: %s", current_i, time_option, selected_table)
          if (time_option != "all" && !is.null(selected_table) && selected_table != "") {
            timestamp_columns <- get_timestamp_columns(selected_table)
            if (length(timestamp_columns) > 0) {
              selectizeInput(
                paste0("timestamp_column_inclusion_", current_i),
                "Order By",
                choices = timestamp_columns,
                selected = timestamp_columns[1]
              )
            } else {
              div(class = "alert alert-warning", "No timestamp columns found in this table")
            }
          }
        })
      })
    }
  })

  output$exclusion_criteria_ui <- renderUI({
    flog.info("Rendering Exclusion Criteria UI.")
    num_criteria <- exclusion_criteria_count()
    criteria_ui <- lapply(1:num_criteria, function(i) {
      fluidRow(
        tags$h5(paste("Exclusion Criteria", i)),
        selectizeInput(paste0("exclusion_criteria_events_", i), "Select Table",
                       choices = c("None" = "", table_choices()), selected = "", multiple = FALSE),
        selectizeInput(paste0("exclusion_column_selection_", i), "Select Column",
                       choices = c("None" = ""), selected = "", multiple = FALSE),
        uiOutput(paste0("exclusion_dynamic_filter_ui_", i))
      )
    })
    do.call(tagList, criteria_ui)
  })

  ### ---------------------- Fetch Column Names Dynamically ---------------------- ###
  update_columns <- function(event_prefix, column_prefix) {
    observe({
      num_criteria <- cohort_criteria_count()
      flog.info("Updating column names for event prefix: %s", event_prefix)
      for (i in 1:num_criteria) {
        local({
          current_i <- i
          observeEvent(input[[paste0(event_prefix, current_i)]], {
            req(input[[paste0(event_prefix, current_i)]])
            selected_cleaned_table <- input[[paste0(event_prefix, current_i)]]
            full_file_names <- names(uploaded_data())
            cleaned_names <- gsub("\\.(csv|parquet)$", "", full_file_names, ignore.case = TRUE)
            selected_table <- full_file_names[which(cleaned_names == selected_cleaned_table)]
            flog.info("Updating columns for table: %s", selected_table)
            selected_path <- uploaded_data()[[selected_table]]
            query <- paste0("SELECT * FROM read_parquet('", selected_path, "') LIMIT 1")
            col_names <- tryCatch({
              dbGetQuery(db_connection, query)
            }, error = function(e) {
              flog.error("Error fetching column names for %s: %s", selected_table, e$message)
              NULL
            })
            if (!is.null(col_names) && ncol(col_names) > 0) {
              filtered_col_names <- colnames(col_names)[!colnames(col_names) %in% c("patient_id", "hospitalization_id")]
              flog.info("Columns for %s: %s", selected_table, toString(filtered_col_names))
              updateSelectizeInput(session, paste0(column_prefix, current_i), choices = filtered_col_names)
            }
          })
        })
      }
    })
  }
  update_columns("cohort_entry_events_", "cohort_column_selection_")
  update_columns("inclusion_criteria_events_", "inclusion_column_selection_")
  update_columns("exclusion_criteria_events_", "exclusion_column_selection_")

  ### ---------------------- Generate Dynamic Filters ---------------------- ###
  # Modified generate_filters now accepts criteria_count and criteria_type
  generate_filters <- function(event_prefix, column_prefix, filter_prefix, criteria_count, criteria_type) {
    observe({
      num_criteria <- criteria_count()
      flog.info("Generating dynamic filters for criteria type: %s", criteria_type)
      for (i in 1:num_criteria) {
        local({
          current_i <- i
          observeEvent(input[[paste0(column_prefix, current_i)]], {
            req(input[[paste0(column_prefix, current_i)]])
            flog.info("Dynamic filter observer triggered for %s, index %s", criteria_type, current_i)
            invalidateLater(1000, session)
            selected_column <- input[[paste0(column_prefix, current_i)]]
            selected_table <- input[[paste0(event_prefix, current_i)]]
            full_file_names <- names(uploaded_data())
            cleaned_names <- gsub("\\.(csv|parquet)$", "", full_file_names, ignore.case = TRUE)
            selected_table_full <- full_file_names[which(cleaned_names == selected_table)]
            type_query <- paste0("PRAGMA table_info(", selected_table, ")")
            column_info <- tryCatch({
              dbGetQuery(db_connection, type_query)
            }, error = function(e) {
              flog.error("Error fetching column info for %s: %s", selected_column, e$message)
              return(NULL)
            })
            if (is.null(column_info) || nrow(column_info) == 0) {
              flog.warn("Column type query returned no results for %s", selected_column)
              return(NULL)
            }
            column_type <- column_info$type[column_info$name == selected_column]
            flog.info("DuckDB inferred type for %s: %s", selected_column, column_type)
            isolate({
              if (is.null(column_type) || length(column_type) == 0) {
                flog.warn("Type inference failed for %s. UI will not be updated.", selected_column)
                return(NULL)
              }
              flog.info("Rendering dynamic filter UI for %s of type %s", selected_column, column_type)
              output[[paste0(filter_prefix, current_i)]] <- renderUI({
                if (grepl("DATE|TIMESTAMP", column_type, ignore.case = TRUE)) {
                  flog.info("Generating Date Filter UI for %s", selected_column)
                  date_range_query <- paste0("
                      SELECT MIN(", selected_column, ") AS min_date, MAX(", selected_column, ") AS max_date 
                      FROM ", selected_table, "
                    ")
                  flog.info("Executing date range query: %s", date_range_query)
                  date_range <- tryCatch({
                    dbGetQuery(db_connection, date_range_query)
                  }, error = function(e) {
                    flog.error("Error fetching date range for %s: %s", selected_column, e$message)
                    return(NULL)
                  })
                  if (!is.null(date_range) && nrow(date_range) > 0) {
                    min_date <- as.Date(date_range$min_date)
                    max_date <- as.Date(date_range$max_date)
                    if (is.na(min_date)) min_date <- Sys.Date() - 365
                    if (is.na(max_date)) max_date <- Sys.Date()
                    flog.info("Min date: %s, Max date: %s", min_date, max_date)
                    dateRangeInput(
                      paste0("date_filter_", criteria_type, "_", current_i),
                      "Select Date Range:",
                      start = min_date,
                      end = max_date,
                      min = min_date - 365,
                      max = max_date + 30
                    )
                  } else {
                    dateRangeInput(paste0("date_filter_", criteria_type, "_", current_i), "Select Date Range:")
                  }
                } else if (grepl("INT|DOUBLE|NUMERIC|FLOAT", column_type, ignore.case = TRUE)) {
                  range_query <- paste0("
                      SELECT MIN(", selected_column, ") AS min_val, MAX(", selected_column, ") AS max_val 
                      FROM ", selected_table, "
                    ")
                  flog.info("Executing numeric range query: %s", range_query)
                  num_range <- tryCatch({
                    dbGetQuery(db_connection, range_query)
                  }, error = function(e) {
                    flog.error("Error fetching numeric range for %s: %s", selected_column, e$message)
                    return(NULL)
                  })
                  if (!is.null(num_range) && nrow(num_range) > 0) {
                    flog.info("Numeric range for %s: %s - %s", selected_column, num_range$min_val, num_range$max_val)
                    sliderInput(
                      paste0("numeric_filter_", criteria_type, "_", current_i),
                      "Select Range:",
                      min = num_range$min_val,
                      max = num_range$max_val,
                      value = c(num_range$min_val, num_range$max_val)
                    )
                  } else {
                    return(NULL)
                  }
                } else {
                  unique_query <- paste0("
                      SELECT DISTINCT ", selected_column, " 
                      FROM ", selected_table, "
                      ORDER BY ", selected_column, "
                    ")
                  flog.info("Executing unique values query: %s", unique_query)
                  unique_values <- tryCatch({
                    dbGetQuery(db_connection, unique_query)[[selected_column]]
                  }, error = function(e) {
                    flog.error("Error fetching unique values for %s: %s", selected_column, e$message)
                    return(NULL)
                  })
                  flog.info("Unique values retrieved for %s: %s", selected_column, toString(head(unique_values, 10)))
                  if (!is.null(unique_values) && length(unique_values) > 0) {
                    selectizeInput(
                      paste0("categorical_filter_", criteria_type, "_", current_i),
                      "Select Values:",
                      choices = unique_values,
                      selected = unique_values[1],
                      multiple = TRUE
                    )
                  } else {
                    return(NULL)
                  }
                }
              })
              outputOptions(output, paste0(filter_prefix, current_i), suspendWhenHidden = FALSE)
            })
          })
        })  # End local
      }  # End for
    })  # End observe
  }  # End generate_filters

  # Call generate_filters with appropriate criteria count and type
  flog.info("Calling generate_filters for cohort, inclusion, and exclusion.")
  generate_filters("cohort_entry_events_", "cohort_column_selection_", "cohort_dynamic_filter_ui_", 
                   cohort_criteria_count, "cohort")
  generate_filters("inclusion_criteria_events_", "inclusion_column_selection_", "inclusion_dynamic_filter_ui_", 
                   inclusion_criteria_count, "inclusion")
  generate_filters("exclusion_criteria_events_", "exclusion_column_selection_", "exclusion_dynamic_filter_ui_", 
                   exclusion_criteria_count, "exclusion")

  # Handle Add/Remove Criteria
  observeEvent(input$add_criteria, {
    flog.info("Add Cohort Criteria triggered.")
    cohort_criteria_count(cohort_criteria_count() + 1)
  })
  observeEvent(input$add_inclusion_criteria, {
    flog.info("Add Inclusion Criteria triggered.")
    inclusion_criteria_count(inclusion_criteria_count() + 1)
  })
  observeEvent(input$add_exclusion_criteria, {
    flog.info("Add Exclusion Criteria triggered.")
    exclusion_criteria_count(exclusion_criteria_count() + 1)
  })
  observeEvent(input$remove_criteria, {
    flog.info("Remove Cohort Criteria triggered.")
    if (cohort_criteria_count() > 1) {
      cohort_criteria_count(cohort_criteria_count() - 1)
    }
  })
  observeEvent(input$remove_inclusion_criteria, {
    flog.info("Remove Inclusion Criteria triggered.")
    if (inclusion_criteria_count() > 1) {
      inclusion_criteria_count(inclusion_criteria_count() - 1)
    }
  })
  observeEvent(input$remove_exclusion_criteria, {
    flog.info("Remove Exclusion Criteria triggered.")
    if (exclusion_criteria_count() > 1) {
      exclusion_criteria_count(exclusion_criteria_count() - 1)
    }
  })

  # Updated get_filter_input_id (with logging)
  get_filter_input_id <- function(table_name, column_name, criteria_type, index) {
    tryCatch({
      flog.info("get_filter_input_id called with Table: %s, Column: %s, Criteria Type: %s, Index: %s", 
                table_name, column_name, criteria_type, index)
      if (is.null(table_name) || table_name == "") {
        flog.error("Error: Table name is NULL or empty")
        return(NULL)
      }
      type_query <- paste0("PRAGMA table_info(", table_name, ")")
      column_info <- dbGetQuery(db_connection, type_query)
      if (is.null(column_info) || nrow(column_info) == 0) {
        flog.error("Error: No columns found for table %s", table_name)
        return(NULL)
      }
      column_type <- column_info$type[column_info$name == column_name]
      if (is.null(column_type) || length(column_type) == 0) {
        flog.error("Error: Column type not found for column %s in table %s", column_name, table_name)
        return(NULL)
      }
      prefix <- if (grepl("DATE|TIMESTAMP", column_type, ignore.case = TRUE)) {
        "date_filter_"
      } else if (grepl("INT|DOUBLE|NUMERIC|FLOAT", column_type, ignore.case = TRUE)) {
        "numeric_filter_"
      } else {
        "categorical_filter_"
      }
      unique_filter_id <- paste0(prefix, criteria_type, "_", index)
      flog.info("Generated unique filter ID: %s for Table: %s, Column: %s, Type: %s",
                unique_filter_id, table_name, column_name, criteria_type)
      return(unique_filter_id)
    }, error = function(e) {
      flog.error("Exception in get_filter_input_id for Table: %s, Column: %s - %s",
                 table_name, column_name, e$message)
      return(NULL)
    })
  }

  # Updated build_condition (with logging)
  build_condition <- function(table_alias, column_name, table_name, criterion_index, criteria_type, inclusion = TRUE) {
    filter_id <- get_filter_input_id(table_name, column_name, criteria_type, criterion_index)
    filter_value <- input[[filter_id]]
    if (is.null(filter_value) || length(filter_value) == 0) return(NULL)
    flog.info("Building condition using filter_id: %s with value: %s", filter_id, toString(filter_value))
    if (grepl("date_filter_", filter_id)) {
      if (length(filter_value) == 2) {
        start_date <- format(as.Date(filter_value[1]), "%Y-%m-%d")
        end_date <- format(as.Date(filter_value[2]), "%Y-%m-%d")
        if (inclusion) {
          return(paste0(table_alias, ".", column_name, " BETWEEN '", start_date, "' AND '", end_date, "'"))
        } else {
          return(paste0("(", table_alias, ".", column_name, " NOT BETWEEN '", start_date, 
                        "' AND '", end_date, "' OR ", table_alias, ".", column_name, " IS NULL)"))
        }
      }
    } else if (grepl("numeric_filter_", filter_id)) {
      if (length(filter_value) == 2) {
        if (inclusion) {
          return(paste0(table_alias, ".", column_name, " BETWEEN ", filter_value[1], " AND ", filter_value[2]))
        } else {
          return(paste0("(", table_alias, ".", column_name, " NOT BETWEEN ", filter_value[1], " AND ", filter_value[2],
                        " OR ", table_alias, ".", column_name, " IS NULL)"))
        }
      }
    } else if (grepl("categorical_filter_", filter_id)) {
      if (length(filter_value) > 0) {
        quoted_values <- paste0("'", gsub("'", "''", filter_value), "'", collapse = ",")
        if (inclusion) {
          return(paste0(table_alias, ".", column_name, " IN (", quoted_values, ")"))
        } else {
          return(paste0("(", table_alias, ".", column_name, " NOT IN (", quoted_values, ") OR ", 
                        table_alias, ".", column_name, " IS NULL)"))
        }
      }
    }
    return(NULL)
  }

  # Generate summaries for each group
  output$cohort_entry_summary <- renderUI({
    flog.info("Rendering Cohort Entry Summary UI.")
    num_criteria <- cohort_criteria_count()
    criteria_summary <- lapply(1:num_criteria, function(i) {
      fluidRow(
        tags$h5(paste("Cohort Entry Event", i)),
        textOutput(paste0("cohort_entry_event_", i))
      )
    })
    do.call(tagList, criteria_summary)
  })

  observe({
    flog.info("Generating Cohort Entry Summary text.")
    num_criteria <- cohort_criteria_count()
    for (i in 1:num_criteria) {
      local({
        current_i <- i
        output[[paste0("cohort_entry_event_", current_i)]] <- renderText({
          selected_event <- input[[paste0("cohort_entry_events_", current_i)]]
          selected_column <- input[[paste0("cohort_column_selection_", current_i)]]
          flog.info("Cohort Entry %s - Table: %s, Column: %s", current_i, selected_event, selected_column)
          if (is.null(selected_event) || selected_event == "") return("No event selected.")
          if (is.null(selected_column) || selected_column == "") return(paste("Event:", selected_event, "| No column selected."))
          filter_id <- get_filter_input_id(selected_event, selected_column, "cohort", current_i)
          if (is.null(filter_id)) return("No event selected.")
          selected_filter <- input[[filter_id]]
          flog.info("Cohort Filter for entry %s: %s", current_i, toString(selected_filter))
          if (is.null(selected_filter) || length(selected_filter) == 0) {
            return(paste("Event:", selected_event, "- Selected Column:", selected_column, "| No filter values selected."))
          }
          paste("Event:", selected_event, "| Selected Column:", selected_column, "| Filter Values:", toString(selected_filter))
        })
      })
    }
  })

  output$inclusion_criteria_summary <- renderUI({
    flog.info("Rendering Inclusion Criteria Summary UI.")
    num_criteria <- inclusion_criteria_count()
    criteria_summary <- lapply(1:num_criteria, function(i) {
      fluidRow(
        tags$h5(paste("Inclusion Criteria", i)),
        textOutput(paste0("inclusion_criteria_event_", i))
      )
    })
    do.call(tagList, criteria_summary)
  })

  observe({
    flog.info("Generating Inclusion Criteria Summary text.")
    num_criteria <- inclusion_criteria_count()
    for (i in 1:num_criteria) {
      local({
        current_i <- i
        output[[paste0("inclusion_criteria_event_", current_i)]] <- renderText({
          selected_event <- input[[paste0("inclusion_criteria_events_", current_i)]]
          selected_column <- input[[paste0("inclusion_column_selection_", current_i)]]
          flog.info("Inclusion Criteria %s - Table: %s, Column: %s", current_i, selected_event, selected_column)
          if (is.null(selected_event) || selected_event == "") return("No event selected.")
          if (is.null(selected_column) || selected_column == "") return(paste("Event:", selected_event, "| No column selected."))
          filter_id <- get_filter_input_id(selected_event, selected_column, "inclusion", current_i)
          if (is.null(filter_id)) return("No event selected.")
          selected_filter <- input[[filter_id]]
          flog.info("Inclusion Filter for criteria %s: %s", current_i, toString(selected_filter))
          if (is.null(selected_filter) || length(selected_filter) == 0) {
            return(paste("Event:", selected_event, "- Selected Column:", selected_column, "| No filter values selected."))
          }
          paste("Event:", selected_event, "| Selected Column:", selected_column, "| Filter Values:", toString(selected_filter))
        })
      })
    }
  })

  output$exclusion_criteria_summary <- renderUI({
    flog.info("Rendering Exclusion Criteria Summary UI.")
    num_criteria <- exclusion_criteria_count()
    criteria_summary <- lapply(1:num_criteria, function(i) {
      fluidRow(
        tags$h5(paste("Exclusion Criteria", i)),
        textOutput(paste0("exclusion_criteria_event_", i))
      )
    })
    do.call(tagList, criteria_summary)
  })

  observe({
    flog.info("Generating Exclusion Criteria Summary text.")
    num_criteria <- exclusion_criteria_count()
    for (i in 1:num_criteria) {
      local({
        current_i <- i
        output[[paste0("exclusion_criteria_event_", current_i)]] <- renderText({
          selected_event <- input[[paste0("exclusion_criteria_events_", current_i)]]
          selected_column <- input[[paste0("exclusion_column_selection_", current_i)]]
          flog.info("Exclusion Criteria %s - Table: %s, Column: %s", current_i, selected_event, selected_column)
          if (is.null(selected_event) || selected_event == "") return("No event selected.")
          if (is.null(selected_column) || selected_column == "") return(paste("Event:", selected_event, "| No column selected."))
          filter_id <- get_filter_input_id(selected_event, selected_column, "exclusion", current_i)
          if (is.null(filter_id)) return("No event selected.")
          selected_filter <- input[[filter_id]]
          flog.info("Exclusion Filter for criteria %s: %s", current_i, toString(selected_filter))
          if (is.null(selected_filter) || length(selected_filter) == 0) {
            return(paste("Event:", selected_event, "| Selected Column:", selected_column, "| No filter values selected."))
          }
          paste("Event:", selected_event, "| Selected Column:", selected_column, "| Filter Values:", toString(selected_filter))
        })
      })
    }
  })

  #################################### 
  cohort_definition_text <- reactiveVal("No cohort definition provided.")

  observeEvent(input$save_definition, {
    flog.info("Saving cohort definition.")
    req(input$cohort_definition)
    cohort_definition_text(input$cohort_definition)
    flog.info("Cohort definition saved: %s", input$cohort_definition)
  })

  output$cohort_definition_display <- renderText({
    flog.info("Displaying cohort definition.")
    cohort_definition_text()
  })

  filtered_data <- reactiveVal(NULL)

  # Build the query for cohort generation
  build_cohort_query <- function() {
    flog.info("Starting build_cohort_query.")
    req(uploaded_data())
    num_cohort <- cohort_criteria_count()
    if (num_cohort < 1) {
      flog.warn("No cohort criteria specified.")
      return(NULL)
    }
    query <- NULL
    where_conditions <- c()
    tables_used <- c()
    table_aliases <- list()
    time_limit_info <- list()
    
    # Function to get common ID columns between two tables
    get_common_id_columns <- function(table1, table2) {
      query1 <- paste0("PRAGMA table_info(", table1, ")")
      query2 <- paste0("PRAGMA table_info(", table2, ")")
      tryCatch({
        col_info1 <- dbGetQuery(db_connection, query1)
        col_info2 <- dbGetQuery(db_connection, query2)
        id_columns1 <- col_info1$name[grep("_id$", col_info1$name)]
        id_columns2 <- col_info2$name[grep("_id$", col_info2$name)]
        if ("patient_id" %in% col_info1$name) id_columns1 <- unique(c(id_columns1, "patient_id"))
        if ("hospitalization_id" %in% col_info1$name) id_columns1 <- unique(c(id_columns1, "hospitalization_id"))
        if ("patient_id" %in% col_info2$name) id_columns2 <- unique(c(id_columns2, "patient_id"))
        if ("hospitalization_id" %in% col_info2$name) id_columns2 <- unique(c(id_columns2, "hospitalization_id"))
        common_columns <- intersect(id_columns1, id_columns2)
        flog.info("Common ID columns between %s and %s: %s", table1, table2, toString(common_columns))
        return(common_columns)
      }, error = function(e) {
        flog.error("Error getting common ID columns: %s", e$message)
        return(NULL)
      })
    }
    
    flog.info("Processing cohort criteria.")
    # Process cohort criteria
    for (i in 1:num_cohort) {
      table_name <- input[[paste0("cohort_entry_events_", i)]]
      column_name <- input[[paste0("cohort_column_selection_", i)]]
      time_option <- input[[paste0("time_limit_option_", i)]]
      timestamp_column <- input[[paste0("timestamp_column_", i)]]
      if (is.null(table_name) || table_name == "" || is.null(column_name) || column_name == "") next
      tables_used <- c(tables_used, table_name)
      table_alias <- ifelse(table_name %in% names(table_aliases),
                            table_aliases[[table_name]],
                            paste0("t", length(table_aliases) + 1))
      table_aliases[[table_name]] <- table_alias
      if (!is.null(time_option) && time_option != "all" && !is.null(timestamp_column) && timestamp_column != "") {
        flog.info("Cohort entry %s has time limit: %s using column: %s", i, time_option, timestamp_column)
        time_limit_info[[table_alias]] <- list(option = time_option, timestamp_column = timestamp_column)
      }
      condition <- build_condition(table_alias, column_name, table_name, i, "cohort", TRUE)
      if (!is.null(condition)) {
        flog.info("Adding condition for cohort entry %s: %s", i, condition)
        where_conditions <- c(where_conditions, condition)
      }
    }
    
    flog.info("Processing inclusion criteria.")
    # Process inclusion criteria (now with time limit support)
    for (i in 1:inclusion_criteria_count()) {
      table_name <- input[[paste0("inclusion_criteria_events_", i)]]
      column_name <- input[[paste0("inclusion_column_selection_", i)]]
      if (is.null(table_name) || table_name == "" || is.null(column_name) || column_name == "") next
      if (!(table_name %in% tables_used)) {
        tables_used <- c(tables_used, table_name)
        table_alias <- paste0("t", length(table_aliases) + 1)
        table_aliases[[table_name]] <- table_alias
      } else {
        table_alias <- table_aliases[[table_name]]
      }
      # Process time limit for inclusion criteria if provided:
      time_option_inclusion <- input[[paste0("time_limit_option_inclusion_", i)]]
      timestamp_column_inclusion <- input[[paste0("timestamp_column_inclusion_", i)]]
      if (!is.null(time_option_inclusion) && time_option_inclusion != "all" &&
          !is.null(timestamp_column_inclusion) && timestamp_column_inclusion != "") {
        flog.info("Inclusion criteria %s has time limit: %s using column: %s", i, time_option_inclusion, timestamp_column_inclusion)
        time_limit_info[[table_alias]] <- list(option = time_option_inclusion,
                                               timestamp_column = timestamp_column_inclusion)
      }
      condition <- build_condition(table_alias, column_name, table_name, i, "inclusion", TRUE)
      if (!is.null(condition)) {
        flog.info("Adding condition for inclusion criteria %s: %s", i, condition)
        where_conditions <- c(where_conditions, condition)
      }
    }
    
    flog.info("Processing exclusion criteria.")
    # Process exclusion criteria
    for (i in 1:exclusion_criteria_count()) {
      table_name <- input[[paste0("exclusion_criteria_events_", i)]]
      column_name <- input[[paste0("exclusion_column_selection_", i)]]
      if (is.null(table_name) || table_name == "" || is.null(column_name) || column_name == "") next
      if (!(table_name %in% tables_used)) {
        tables_used <- c(tables_used, table_name)
        table_alias <- paste0("t", length(table_aliases) + 1)
        table_aliases[[table_name]] <- table_alias
      } else {
        table_alias <- table_aliases[[table_name]]
      }
      condition <- build_condition(table_alias, column_name, table_name, i, "exclusion", FALSE)
      if (!is.null(condition)) {
        flog.info("Adding condition for exclusion criteria %s: %s", i, condition)
        where_conditions <- c(where_conditions, condition)
      }
    }
    
    if (length(tables_used) == 0) {
      flog.warn("No tables specified in cohort criteria.")
      return(NULL)
    }
    
    flog.info("Building FROM clause with tables: %s", toString(tables_used))
    from_clause <- paste0("FROM ", tables_used[1], " AS ", table_aliases[[tables_used[1]]])
    if (length(tables_used) > 1) {
      for (i in 2:length(tables_used)) {
        for (j in 1:(i-1)) {
          common_columns <- get_common_id_columns(tables_used[j], tables_used[i])
          if (length(common_columns) > 0) {
            join_column <- common_columns[1]
            from_clause <- paste0(from_clause, "\nLEFT JOIN ", tables_used[i], " AS ", table_aliases[[tables_used[i]]],
                                  " ON ", table_aliases[[tables_used[j]]], ".", join_column, " = ",
                                  table_aliases[[tables_used[i]]], ".", join_column)
            break
          }
        }
      }
    }
    
    flog.info("Building SELECT clause.")
    select_clause <- "SELECT"
    for (table_name in tables_used) {
      alias <- table_aliases[[table_name]]
      select_clause <- paste0(select_clause, "\n  ", alias, ".*,")
    }
    select_clause <- substr(select_clause, 1, nchar(select_clause) - 1)
    
    where_clause <- ""
    if (length(where_conditions) > 0) {
      where_clause <- paste0("\nWHERE ", paste(where_conditions, collapse = " AND "))
    }
    
    base_query <- paste0(select_clause, "\n", from_clause, where_clause)
    flog.info("Base query constructed:\n%s", base_query)
    
    # Now check if time limits should be applied (for cohort and/or inclusion criteria)
    if (length(time_limit_info) > 0) {
      flog.info("Time limit info present (%s entries), applying time limits.", length(time_limit_info))
      query <- "WITH base_data AS (\n"
      query <- paste0(query, base_query, "\n)")
      
      # Determine partition column (using cohort table, for example)
      has_hospitalization_id <- TRUE
      for (table_name in tables_used) {
        type_query <- paste0("PRAGMA table_info(", table_name, ")")
        column_info <- dbGetQuery(db_connection, type_query)
        if (!("hospitalization_id" %in% column_info$name)) {
          has_hospitalization_id <- FALSE
          break
        }
      }
      has_patient_id <- TRUE
      for (table_name in tables_used) {
        type_query <- paste0("PRAGMA table_info(", table_name, ")")
        column_info <- dbGetQuery(db_connection, type_query)
        if (!("patient_id" %in% column_info$name)) {
          has_patient_id <- FALSE
          break
        }
      }
      partition_column <- if (has_hospitalization_id) {
        "hospitalization_id"
      } else if (has_patient_id) {
        "patient_id"
      } else {
        # Fallback: use the primary key of the first table or first column
        first_table <- tables_used[1]
        type_query <- paste0("PRAGMA table_info(", first_table, ")")
        column_info <- dbGetQuery(db_connection, type_query)
        primary_keys <- column_info$name[column_info$pk > 0]
        if (length(primary_keys) > 0) {
          primary_keys[1]
        } else {
          column_info$name[1]
        }
      }
      flog.info("Using partition column for time limit: %s", partition_column)
      
      order_by_parts <- c()
      for (table_alias in names(time_limit_info)) {
        info <- time_limit_info[[table_alias]]
        timestamp_col <- paste0(table_alias, ".", info$timestamp_column)
        # For generalization, use unqualified column name
        unqualified_col <- sub(".*\\.", "", timestamp_col)
        flog.info("Ordering by unqualified column %s for alias %s with option %s", unqualified_col, table_alias, info$option)
        if (info$option == "earliest") {
          order_by_parts <- c(order_by_parts, paste0(unqualified_col, " ASC"))
        } else {
          order_by_parts <- c(order_by_parts, paste0(unqualified_col, " DESC"))
        }
      }
      order_by_clause <- paste(order_by_parts, collapse = ", ")
      
      rank_query <- paste0(
        "WITH base_data AS (\n", base_query, "\n)\n",
        "SELECT * FROM (\n",
        "  SELECT *, ROW_NUMBER() OVER (PARTITION BY ", partition_column, " ORDER BY ", 
        order_by_clause,
        ") AS row_num\n",
        "  FROM base_data\n",
        ") AS ranked_query\n",
        "WHERE row_num = 1"
      )
      
      flog.info("Ranked query constructed:\n%s", rank_query)
      return(rank_query)
    } else {
      flog.info("No time limits applied. Returning base query.")
      return(base_query)
    }
  }

  output$row_count <- renderUI({
    req(filtered_data())
    count <- nrow(filtered_data())
    flog.info("Rendering row count: %s", count)
    tags$h4(style = "text-align: left; margin: 20px 0;",
            paste("Records meeting criteria:", count))
  })

  observeEvent(input$generate_summary, {
    flog.info("Generate Summary button clicked.")
    req(uploaded_data())
    query <- build_cohort_query()
    if (is.null(query)) {
      flog.warn("Query is NULL. Check cohort criteria.")
      showNotification("Please select cohort criteria before generating summary.", type = "warning")
      return(NULL)
    }
    flog.info("Executing query: %s", query)
    tryCatch({
      result <- dbGetQuery(db_connection, query)
      flog.info("Query executed successfully. Rows returned: %s", nrow(result))
      filtered_data(result)
      summary_stats <- generate_summary_stats(result)
      if (!is.null(summary_stats)) {
        summary_df <- as.data.frame(summary_stats, stringsAsFactors = FALSE)
        if (!"Variable" %in% colnames(summary_df)) {
          summary_df$Variable <- rownames(summary_df)
          rownames(summary_df) <- NULL
        }
        summary_df <- summary_df[, c("Variable", "Overall")]
        flog.info("Summary stats generated with %s rows.", nrow(summary_df))
        summary_data(summary_df)
        output$summary_stats_table <- DT::renderDataTable({
          req(summary_data())
          DT::datatable(
            summary_data(),
            options = list(pageLength = 10, dom = 'ltp', lengthMenu = list(c(10, 25, 50, -1), c('10', '25', '50', 'All')),
                            class = 'table table-striped table-bordered', rownames = FALSE,
                            ordering = FALSE, searching = FALSE),
            class = 'table table-striped table-bordered'
          )
        })
  output$row_count <- renderUI({
    unique_count <- length(unique(filtered_data()[["hospitalization_id"]]))
    div(class = "alert alert-info", style = "margin-bottom: 20px;",
        paste("Number of unique hospitalization IDs meeting criteria:", 
              format(unique_count, big.mark = ",")))
  })


      } else {
        flog.warn("No summary statistics could be generated.")
        showNotification("No summary statistics could be generated.", type = "warning")
      }
    }, error = function(e) {
      flog.error("Error executing query: %s", e$message)
      showNotification("Error generating summary. Please check your criteria.", type = "error")
    })
  })

  output$filtered_data_table <- DT::renderDataTable({
    req(filtered_data())
    flog.info("Rendering filtered data table with %s rows.", nrow(filtered_data()))
    DT::datatable(filtered_data(), options = list(pageLength = 3))
  })

  generate_summary_stats <- function(data) {
    flog.info("Generating summary statistics.")
    if (is.null(data) || nrow(data) == 0) {
      flog.warn("No data available for summary generation.")
      return(NULL)
    }
    numeric_cols <- sapply(data, is.numeric)
    categorical_cols <- sapply(data, is.factor) | sapply(data, is.character)
    all_cols <- colnames(data)[numeric_cols | categorical_cols]
    # Exclude specific columns and any column containing "_id"
    exclude <- c("patient_id", "hospitalization_id", "zipcode_five_digit",
                "zipcode_nine_digit", "census_block_group", "latitude", "longitude")
    all_cols <- setdiff(all_cols, exclude)
    all_cols <- all_cols[!grepl("_id", all_cols, ignore.case = TRUE)]
    if (length(all_cols) == 0) {
      flog.warn("No numeric or categorical columns available for summary.")
      return(data.frame(Message = "No numeric or categorical columns available for summary"))
    }
    flog.info("Creating TableOne object for columns: %s", toString(all_cols))
    table_one <- CreateTableOne(vars = all_cols, data = data, test = TRUE)
    summary_stats <- print(table_one, printToggle = FALSE, quote = FALSE)
    flog.info("Summary statistics generation completed.")
    return(summary_stats)
  }

  summary_data <- reactiveVal(NULL)

  output$export_data <- downloadHandler(
    filename = function() {
      paste("exported_data_", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      flog.info("Exporting data as CSV.")
      req(filtered_data())
      export_data <- unique(filtered_data()[, c("hospitalization_id", "patient_id"), drop = FALSE])
      export_data <- na.omit(export_data)
      if (nrow(export_data) == 0) {
        flog.error("No data available to export.")
        stop("No data available to export. Please ensure that either hospitalization_id or patient_id exists.")
      }
      write.csv(export_data, file, row.names = FALSE)
    }
  )

  output$download_summary <- downloadHandler(
    filename = function() {
      paste("tableone_summary_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".html", sep = "")
    },
    content = function(file) {
      flog.info("Exporting summary statistics as HTML.")
      req(summary_data())
      summary_df <- as.data.frame(summary_data(), stringsAsFactors = FALSE)
      html_content <- paste0('
        <!DOCTYPE html>
        <html>
        <head>
          <style>
            body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .header { background-color: #2c3e50; color: white; padding: 20px; margin-bottom: 20px; border-radius: 5px; text-align: center; }
            .metadata { background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px; border-left: 4px solid #2c3e50; }
            .summary-table { width: 100%; border-collapse: collapse; margin-top: 20px; background-color: white; }
            .summary-table th { background-color: #2c3e50; color: white; padding: 12px; text-align: left; font-weight: bold; }
            .summary-table td { padding: 10px; border: 1px solid #ddd; }
            .summary-table tr:nth-child(even) { background-color: #f8f9fa; }
            .summary-table tr:hover { background-color: #f1f1f1; }
            .timestamp { color: #95a5a6; font-size: 0.9em; margin-top: 5px; }
            .cohort-def { font-weight: bold; color: #2c3e50; }
            .section-header { color: #2c3e50; margin-top: 20px; padding-bottom: 5px; border-bottom: 2px solid #2c3e50; }
          </style>
        </head>
        <body>
          <div class="container">
            <div class="header">
              <h2>Cohort Summary Statistics</h2>
              <div class="timestamp">Generated on: ', format(Sys.time(), "%Y-%m-%d %H:%M:%S"), '</div>
            </div>
            <div class="metadata">
              <h3>Cohort Definition</h3>
              <p class="cohort-def">', cohort_definition_text(), '</p>
            </div>
            <div class="summary">
              <h3 class="section-header">Summary Statistics</h3>
              <table class="summary-table">
                <thead>
                  <tr>
                    <th>Variable</th>
                    <th>Overall</th>
                  </tr>
                </thead>
                <tbody>')
      for (i in 1:nrow(summary_df)) {
        html_content <- paste0(html_content, '
          <tr>
            <td>', summary_df[i, "Variable"], '</td>
            <td>', summary_df[i, "Overall"], '</td>
          </tr>')
      }
      html_content <- paste0(html_content, '
                </tbody>
              </table>
            </div>
          </div>
        </body>
        </html>')
      flog.info("Writing summary HTML to file.")
      write(html_content, file)
    }
  )

  output$download_cohort_definition <- downloadHandler(
    filename = function() {
      paste("cohort_definition_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".json", sep = "")
    },
    content = function(file) {
      flog.info("Exporting cohort definition as JSON.")
      cohort_info <- list(
        cohort_definition = cohort_definition_text(),
        cohort_entries = lapply(1:cohort_criteria_count(), function(i) {
          list(
            table = input[[paste0("cohort_entry_events_", i)]],
            column = input[[paste0("cohort_column_selection_", i)]],
            filter = input[[ 
              get_filter_input_id(
                input[[paste0("cohort_entry_events_", i)]],
                input[[paste0("cohort_column_selection_", i)]],
                "cohort", 
                i
              )
            ]]
          )
        }),
        inclusion_criteria = lapply(1:inclusion_criteria_count(), function(i) {
          list(
            table = input[[paste0("inclusion_criteria_events_", i)]],
            column = input[[paste0("inclusion_column_selection_", i)]],
            filter = input[[ 
              get_filter_input_id(
                input[[paste0("inclusion_criteria_events_", i)]],
                input[[paste0("inclusion_column_selection_", i)]],
                "inclusion", 
                i
              )
            ]]
          )
        }),
        exclusion_criteria = lapply(1:exclusion_criteria_count(), function(i) {
          list(
            table = input[[paste0("exclusion_criteria_events_", i)]],
            column = input[[paste0("exclusion_column_selection_", i)]],
            filter = input[[ 
              get_filter_input_id(
                input[[paste0("exclusion_criteria_events_", i)]],
                input[[paste0("exclusion_column_selection_", i)]],
                "exclusion", 
                i
              )
            ]]
          )
        }),
        timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      )
      
      json_content <- jsonlite::toJSON(cohort_info, pretty = TRUE)
      flog.info("Cohort definition JSON generated.")
      write(json_content, file)
    }
  )

  onStop(function() {
    flog.info("Shutting down: Disconnecting from DuckDB.")
    dbDisconnect(db_connection)
  })


  # Function to format table rows (if needed)
  format_table_rows <- function(data) {
    is_key_row <- !grepl("^\\s+", data[[1]])
    rowClass <- ifelse(is_key_row, 'key-row', 'sub-row')
    DT::datatable(
      data,
      class = 'table table-striped table-bordered',
      options = list(pageLength = 50, dom = 't'),
      rownames = FALSE,
      selection = 'none',
      options = list(
        rowCallback = JS("
          function(row, data, index) {
            $(row).addClass('", rowClass[index + 1], "');
          }
        ")
      )
    )
  }
}
