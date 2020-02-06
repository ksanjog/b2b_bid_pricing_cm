server <- function(input, output, session) {
  
  
  output$fiscal_selection <- renderUI({selectInput(
    inputId = "fiscal",
    label = "Fiscal Year",
    choices = unique(bl$fiscal),
    selected = '2019'
  )})
  
  output$dest_selection <- renderUI({
    req(input$fiscal)
    selectInput(inputId = "destination",
                label = "Destination",
                # choices = as.vector(as.character(unique(bl[bl$fiscal == input$fiscal,]$dest))),
                choices = unique(bl[bl$fiscal == input$fiscal,]$dest),
                selected = 'H761519'
                )})
  
  output$bidline_selection <- renderUI({
    req(input$fiscal, input$destination)
    selectInput(inputId = "bidlineno",
                label = "Bidline Number",
                # choices = as.numeric(as.character(unique(bl[bl$fiscal == input$fiscal & bl$dest == input$destination,]$dest))),
                choices = unique(bl[bl$fiscal == input$fiscal & bl$dest == input$destination,]$bidline),
                selected = '42500'
  )})
  
  
  output$hcontainer_1 <- renderHighchart({
    hc_1 <- highchart() %>%
      hc_add_series(
        data = imp_df,
        hcaes(y = importance, x = variable),
        name = "Feature Importance",
        type = "bar"
      ) %>%
      hc_xAxis(title = "Feature Names",
               categories = imp_df$variable) %>%
      hc_yAxis(title = "Relative Importance (%)",
               labels = list(enabled = TRUE,
                             format = '{value} %')) %>%
      hc_tooltip(
        pointFormat = "Importance (%) : {point.y}",
        headerFormat =  '',
        crosshairs = TRUE
      )
    
    hc_1
  })
  
  output$hcontainer_2 <- renderHighchart({
    req(input$fiscal,input$destination,input$bidlineno)
    
    hc <- highchart() %>%
      hc_add_series(
        data = data.frame(
          "x" = seq(0.2, 100, 1 / 5),
          "y" = sort(as.numeric(tree_pred[tree_pred$fiscal == input$fiscal & tree_pred$destination == input$destination & tree_pred$bidlineno == input$bidlineno, c(4:503)]), decreasing = TRUE)
        ),
        hcaes(x, y),
        name = "Price vs Probability",
        type = "spline"
      ) %>%
      hc_xAxis(title = "Winning Probability (%)",
               labels = list(enabled = TRUE,
                             format = '{value} %')) %>%
      hc_yAxis(title = "Bid Price USD", labels = list(enabled = TRUE)) %>%
      hc_tooltip(
        pointFormat = "Probability (%): {point.x} <br> Price ($/T): {point.y}",
        headerFormat =  '',
        crosshairs = TRUE
      )
    hc
  })
  
  output$price_recom_tbl <- renderRHandsontable({
    req(input$fiscal,input$destination,input$bidlineno)
    rhandsontable(
      data = price_recommendation_df[price_recommendation_df$fiscal == input$fiscal & 
                                       price_recommendation_df$destination == input$destination & 
                                       price_recommendation_df$bidlineno == input$bidlineno, c("Category", "Price", "Margin")],
      selectCallback = FALSE,
      useTypes = FALSE,
      stretchH = "all",
      rowHeaders = F,
      search = FALSE
    )
  })
  
  output$bid_line_tbl <- renderDataTable({
    req(input$destination,input$bidlineno)
    datatable(
      data =
        clust_reftable[clust_reftable$destination_no == input$destination & clust_reftable$bid_line_no == input$bidlineno, 
                       c('City & State',
                         'Date',
                         'Volume (K Tons)',
                         'CMP Won Bid')],
      extensions = 'Buttons',
      rownames = FALSE,
      autoHideNavigation = TRUE,
      class = "display",
      options = list(
        pageLength = 5,
        dom = 'Bfrtip',
        buttons = c('copy', 'csv', 'excel')
      )
    )
  }, server = FALSE)
  
  output$ref_tbl <- renderRHandsontable({
    req(input$fiscal,input$destination,input$bidlineno)
    rhandsontable(
      data = unique(clust_reftable[(clust_reftable$Cluster == as.numeric(unique(clust_reftable[clust_reftable$fiscal == input$fiscal & 
                                                                                          clust_reftable$destination_no == input$destination & 
                                                                                          clust_reftable$bid_line_no == input$bidlineno, "Cluster"]))) & clust_reftable$fiscal == 2019, c(
        'Customer Name',
        'City & State',
        'Volume (K Tons)',
        'Date',
        'CMP Won Bid',
        'CMP Bid Price',
        'Top Competitor',
        'Top Competitor Bid Price',
        'Bid Price Last Year',
        'Delta YoY AwardPrice',
        'Incumbent'
      )])[1:5,],
      selectCallback = FALSE,
      useTypes = FALSE,
      stretchH = "all",
      rowHeaders = F,
      search = FALSE
    )
  })
  
  output$perf_tbl_1 <- renderRHandsontable({
    req(input$fiscal,input$destination,input$bidlineno)
    rhandsontable(
      data = opt_1[opt_1$fiscal == input$fiscal & 
                     opt_1$destination == input$destination & 
                     opt_1$bidlineno == input$bidlineno, c('Price', 'This Year', 'Last Year')],
      selectCallback = FALSE,
      useTypes = FALSE,
      stretchH = "all",
      rowHeaders = F,
      search = FALSE
    )
  })
  
  output$perf_tbl_2 <- renderRHandsontable({
    req(input$fiscal,input$destination,input$bidlineno)
    rhandsontable(
      data = opt_2[opt_2$fiscal == input$fiscal & 
                     opt_2$destination == input$destination & 
                     opt_2$bidlineno == input$bidlineno, c('Price', 'This Year', 'Last Year')],
      selectCallback = FALSE,
      useTypes = FALSE,
      stretchH = "all",
      rowHeaders = F,
      search = FALSE
    )
  })
  
  
}
