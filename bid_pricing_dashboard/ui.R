ui = tabsetPanel(tabPanel(title = "Bid Price Predictions",
                          fluidPage(
                            fluidRow(
                              column(
                                width = 8,
                                titlePanel(title = 'BID ANALYSIS TOOL',
                                           windowTitle = 'Compass Minerals:Bid Analysis Tool')
                              ),
                              column(
                                width = 2,
                                img(
                                  src = 'compass_logo.png',
                                  align = "right",
                                  height = 48,
                                  width = 150
                                )
                              )
                            ),
                            
                            fluidRow(
                              column(width = 1.25,
                                     h3('Select Bid to Analyse')
                                     )
                                    ),
                            
                            fluidRow(
                              # Select fiscal dropdown
                              column(
                                width = 1,
                                uiOutput("fiscal_selection")
                              ),
                              
                              # Select destination dropdown
                              column(
                                width = 1,
                                uiOutput("dest_selection")
                              ),
                              
                              # Select bidlineno dropdown
                              column(
                                width = 1,
                                uiOutput("bidline_selection")
                              )),
                            
                            
                            
                            fluidRow(
                              column(width = 4,
                                     fluidRow(h3('Overall Performance - State'),
                                              rHandsontableOutput("perf_tbl_1")),
                                     fluidRow(h3('Top Driver Significance'),
                                              highchartOutput("hcontainer_1"))
                                    ),
                                     
                              column(width = 4,
                                     fluidRow(h3('National'),
                                              rHandsontableOutput("perf_tbl_2")),
                                     fluidRow(h3('Estimated Win Probabilty Chart'),
                                              highchartOutput("hcontainer_2"))
                                    ),
                              column(width = 4,
                                     fluidRow(h3('Price Recommendations'),
                                              rHandsontableOutput("price_recom_tbl")),
                                     fluidRow(h3('Bid Line Detail'),
                                              dataTableOutput("bid_line_tbl"))
                                     )
                                    ),
                            fluidRow(
                              column(width = 12,
                                     h3('For reference: Recent similar bids overview'),
                                     rHandsontableOutput("ref_tbl"))
                                    )
                          )))
