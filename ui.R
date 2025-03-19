ui <- fluidPage(
  tags$head(
    tags$style(HTML("
      /* Overall app styling */
      body {
        background-color: #f5f7fa;
        font-family: 'Helvetica Neue', Arial, sans-serif;
      }
      
      /* Title styling */
      .title-panel {
        background-color: #2c3e50;
        color: white;
        padding: 25px 0;
        margin-bottom: 40px;
        border-radius: 0 0 10px 10px;
        width: 100%;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
      }
      
      .title-panel h2 {
        font-weight: 300;
        letter-spacing: 2.5px;
        margin: 0;
        text-align: center;
      }
      
      /* File input styling */
      .file-input-container {
        background: white;
        padding: 25px;
        border-radius: 10px;
        box-shadow: 0 2px 15px rgba(0,0,0,0.05);
        transition: all 0.3s ease;
        display: flex;
        flex-direction: column;
        align-items: center;
      }
      
      .file-input-container:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
      }
      
      .btn-file {
        background-color: #3498db;
        color: white;
        border: none;
        border-radius: 5px;
        padding: 6px 12px;
        font-size: 14px;
        transition: all 0.3s ease;
      }
      
      .btn-file:hover {
        background-color: #2980b9;
      }
      
      /* File input label */
      .control-label {
        color: #34495e;
        font-weight: 500;
        margin-bottom: 15px;
        font-size: 16px;
        text-align: center;
      }
      
      /* Progress bar styling */
      .progress-bar {
        background-color: #2ecc71;
      }
      
      /* Loading spinner styling */
      .loading-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        min-height: 200px;
        background: white;
        border-radius: 10px;
        box-shadow: 0 2px 15px rgba(0,0,0,0.05);
        margin: 20px 0;
      }
      
      .loading-spinner {
        border: 4px solid #f3f3f3;
        border-top: 4px solid #3498db;
        border-radius: 50%;
        width: 40px;
        height: 40px;
        animation: spin 1s linear infinite;
        margin-bottom: 15px;
      }
      
      .loading-text {
        color: #34495e;
        font-size: 16px;
        margin-top: 10px;
      }
      
      @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
      }
      
      /* Logo styling */
      .logo-container {
        display: flex;
        align-items: left;
        justify-content: center;
        gap: 20px;
      }
      
      .logo-img {
        height: 50px;
        width: auto;
      }
      
      .logo-title {
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 2em;
      }
      
      .logo-img {
        height: 2em;
        width: auto;
        margin: 0 2px;
        vertical-align: middle;
      }
      
      /* Center file input elements */
      .file-input-container .form-group {
        width: 300px;
        display: flex;
        flex-direction: column;
        align-items: center;
      }
      
      .file-input-container .control-label {
        text-align: center;
        margin-bottom: 15px;
        width: 100%;
      }
      
      .file-input-container .input-group {
        display: flex;
        width: 100%;
        position: relative;
      }
      
      .file-input-container .form-control {
        margin-left: 100px;
        border-radius: 5px;
        width: 55%;
      }
      
      .file-input-container .btn-file {
        border-radius:5px;
        right: -20px;
        top: 0;
        height: 105%;
        z-index: 2;
      }
      
      /* Instructions panel styling */
      .instructions-panel {
        background: #eaf2f8;
        padding: 20px;
        border-radius: 8px;
        margin-top: 20px;
        width: 100%;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
      }
      
      .instructions-panel h4 {
        color: #2c3e50;
        margin-bottom: 10px;
      }
      
      .instructions-panel p {
        color: #34495e;
        font-size: 14px;
        line-height: 1.5;
      }
    "))
  ),
  
  shinyjs::useShinyjs(),
  
  # Title Panel
  div(class = "title-panel",
      fluidRow(
        column(12,
               h2(class = "logo-title",
                  "CLIF Cohort Disc",
                  img(src = "logo.png", class = "logo-img"),
                  "very"
               )
        )
      )
  ),
  
  # File Input Panel with Instructions Panel
  fluidRow(
    column(6, offset = 3,
      div(class = "file-input-container",
        fileInput("main", 
          label = div(
            style = "text-align: center; width: 100%;",
            icon("file-upload", class = "fa-lg"), 
            "Select one or more files"
          ),
          multiple = TRUE,
          accept = c(".csv", ".parquet", ".fst"),
          buttonLabel = "Browse..."
        ),
        div(class = "instructions-panel",
            h4("Instructions"),
            p("1. Please give the app a minute to load after you have uploaded your files."),
            p("2. If using different files (tables) for different criteria (e.g., cohort entry events, inclusion or exclusion criteria), each file must have a common ID (such as 'hospitalization_id' or 'patient_id') to allow proper joining of the tables."),
            p("3. When setting the initial event limit for cohort entry or inclusion criteria, for this to work correctly, the table must include a 'hospitalization_id' and a date/time (dttm) column that will be used to order events.")
        )
      )
    )
  ),
  
  # Conditional Panels for Processing/Results
  conditionalPanel(
    condition = "output.filesUploaded == true",
    
    # Loading spinner (shown while data is being processed)
    conditionalPanel(
      condition = "output.dataReady != true",
      div(class = "loading-container",
          div(class = "loading-spinner"),
          div(class = "loading-text", "Loading...")
      )
    ),
    
    # TabsetPanel (shown when data is ready)
    conditionalPanel(
      condition = "output.dataReady == true",
      tabsetPanel(
        tabPanel("Cohort Entry",
                 fluidRow(
                   column(12,
                          wellPanel(
                            tags$h4("Define Cohort Entry Event"),
                            textAreaInput("cohort_definition",
                                          label = NULL,
                                          placeholder = "Enter your cohort definition here...",
                                          rows = 3),
                            actionButton("save_definition", "Save Definition")
                          )
                   )
                 ),
                 
                 hr(),
                 fluidRow(
                   column(4,
                          wellPanel(
                            tags$h4("Cohort Entry Events"),
                            uiOutput("cohort_entry_ui"),
                            actionButton("add_criteria", "+ Add Event"),
                            actionButton("remove_criteria", "- Remove Last Event", class = "btn-warning")
                          )
                   ),
                   
                   column(4,
                          wellPanel(
                            tags$h4("Inclusion Criteria"),
                            uiOutput("inclusion_criteria_ui"),
                            actionButton("add_inclusion_criteria", "+ Add Criteria"),
                            actionButton("remove_criteria", "- Remove Last Criteria", class = "btn-warning")
                          )
                   ),
                   
                   column(4,
                          wellPanel(
                            tags$h4("Exclusion Criteria"),
                            uiOutput("exclusion_criteria_ui"),
                            actionButton("add_exclusion_criteria", "+ Add Criteria"),
                            actionButton("remove_criteria", "- Remove Last Criteria", class = "btn-warning")
                          )
                   )
                 ),
                 fluidRow(
                   column(4, offset = 4, align = "center",
                          actionButton("generate_summary", "Generate Summary"),
                          tags$br(),
                          tags$br()
                   )
                 ),
                 fluidRow(
                   column(12,
                          wellPanel(
                            tags$h4("Cohort Entry Summary"),
                            uiOutput("cohort_entry_summary")
                          )
                   )
                 ),
                 fluidRow(
                   column(12,
                          wellPanel(
                            tags$h4("Inclusion Criteria Summary"),
                            uiOutput("inclusion_criteria_summary")
                          )
                   )
                 ),
                 fluidRow(
                   column(12,
                          wellPanel(
                            tags$h4("Exclusion Criteria Summary"),
                            uiOutput("exclusion_criteria_summary")
                          )
                   )
                 )
        ),
        tabPanel("Summary",
                 tags$h3("Cohort Summary"),
                 tags$br(),
                 tags$h4("Cohort Definition"),
                 textOutput("cohort_definition_display"),
                 tags$br(),
                 tags$br(),
                 tags$h4("Cohort Preview"),
                 div(
                   style = "margin: 20px 0;",
                   uiOutput("row_count")
                 ),
                 div(class = "table-responsive",
                     DT::dataTableOutput("filtered_data_table")
                 ),
                 tags$br(),
                 tags$br(),
                 tags$h4("Summary Statistics"),
                 div(class = "table-responsive",
                     DT::dataTableOutput("summary_stats_table")
                 )
        ),
        tabPanel("Export",
                 tags$h4("Download cohort definition in JSON format"),
                 tags$h6("Includes cohort entry events, inclusion, and exclusion criteria." ),
                 tags$h6(
                   style = "font-style: italic;",
                   "Please ensure you have defined and saved your cohort definition to enable download."
                 ),
                 downloadButton("download_cohort_definition", "Download Definition"),
                 tags$br(),
                 tags$br(),
                 tags$h4("Export cohort data as a CSV file"),
                 downloadButton("export_data", "Export Data"),
                 tags$br(),
                 tags$br(),
                 tags$h4("Export Cohort Summary Statistics as an HTML"),
                 downloadButton("download_summary", "Download Summary")
        )
      )
    )
  )
)
