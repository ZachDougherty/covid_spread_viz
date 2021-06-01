# Visualization for the spread of COVID in the continental US
![week40](https://github.com/covid_spread_viz/imgs/daily_week40.png)

Using data from the New York Times on new COVID cases from each state in the US, I constructed maps for each week of 2020. I aggregated and munged the data which produced the `states_by_cases` text file in this repository. Using matplotlib and Basemap, I constructed 50 plots, one for each week of available data, and then used a screen recording of all the images in succession to emulate the effect of 'animation', the result of which is `covid_spread_colorbar.mov`. The `shapes` directory contains the shape files necessary for Basemap to properly construct the ma and boundaries of the United States. This was originally developed in a Jupyter Notebook which I reworked into a python script for ease of use.

Thank you for taking the time to check out my visualization, any feedback is appreciated!

Data source: https://github.com/nytimes/covid-19-data
