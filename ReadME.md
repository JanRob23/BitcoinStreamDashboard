### Project Idea
Stream trades of bitcoin in different currencies using a kafka producer, this can either be one stream for all currencies or one stream per currency need to figure whats better. 
Use a kafka consumer to store the streamed data into bigquery in batches (say every hour), here I need to define the table stucture and maybe do some optimization based on upstream queries. 
Display the data in some interactive plots using streamlit, here I should decide what queries I want to use:
    - for each currency: Number of buys / sales per time unit (say per minute, 10mins, hour)
    - for each currency: 