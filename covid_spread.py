import pyspark
import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap as Basemap
from matplotlib.patches import Polygon
from matplotlib.colorbar import ColorbarBase

""" Spark set up """
sc = pyspark.SparkContext().getOrCreate()

rdd = sc.textFile("states_by_weeks")
fixed_rdd = rdd.map(lambda x: x[1:-1].split(',')).map(lambda x: (x[0].strip("''"), int(x[1].strip()), float(x[2].strip()), float(x[3].strip()))).cache()

""" Construct the Data """
all_states = sorted(fixed_rdd.map(lambda x: x[0]).distinct().collect())

# removing all non-mainland regions
for region in ['Guam','Northern Mariana Islands','Virgin Islands']: 
    all_states.remove(region)

all_weeks = sorted(fixed_rdd.map(lambda x: x[1]).distinct().collect())

# build dataframe with every state/ week combination
states_COL = []
weeks_COL = []

num_wks = len(all_weeks)

for state in all_states:
    states_COL.extend(
        (f'{state},'*num_wks).split(',')[:-1]
    )
    
num_states = len(all_states)

for _ in range(num_states):
    weeks_COL.extend(all_weeks)
    
assert len(states_COL) == len(weeks_COL)

df = pd.DataFrame(
    {
        'state': states_COL,
        'week': weeks_COL
    }
)

rdd_df = pd.DataFrame(fixed_rdd.collect(), columns=['state','week','daily_cases','cumulative_cases'])

complete = df.merge(rdd_df, how='left', on=['week','state']).fillna(0)

""" Plotting """
colors = [
    '#fee5d9',
    '#fcbba1',
    '#fc9272',
    '#fb6a4a',
    '#ef3b2c',
    '#cb181d',
    '#99000d',
]

complete.loc[:,'daily_color'] = pd.qcut(complete.daily_cases, 7, labels=colors)

# create the maps
for wk in all_weeks:
    
    wk_df = complete.loc[complete.week == wk, :]
    state_colors = { row['state']: row['daily_color'] for i, row in wk_df.iterrows() }
    state_colors.update({'Puerto Rico': '#000000'})
    
    # create the projection
    m = Basemap(llcrnrlon=-119, llcrnrlat=22, urcrnrlon=-64, urcrnrlat=49,
               projection='lcc', lat_1=33, lat_2=45, lon_0=-95)

    # load the shapefile, using name states
    shape = m.readshapefile('shape_files/st99_d00',
                            name='states', drawbounds=True,
                           linewidth=.7, color='#808080') # get better color
    
    ax = plt.gca() # get current axes instance
    fig = plt.gcf()

    for _, spine in ax.spines.items():
        spine.set_visible(False)

    # list of states in data
    states = [shapedict['NAME'] for shapedict in m.states_info]

    for i, seg in enumerate(m.states):
        state = states[i]
        color = state_colors[state]
        poly = Polygon(seg, facecolor=color, edgecolor=color)
        ax.add_patch(poly)
        
    cb_ax = fig.add_axes([.95,.17,.1,.6])

    cmap = mpl.colors.ListedColormap(colors)

    bounds = list(range(1,9))
    boundaries = [
    		'0',
    		'9',
    		'497',
    		'1,648',
    		'3,842',
    		'6,613',
    		'14,546',
    		'299,158'
    ]

    cb = ColorbarBase(cb_ax, cmap=cmap,
                      boundaries=bounds,
                      ticks=bounds,
                      label=boundaries,
                      orientation='vertical')

    cb.set_label('')
    # true label
    cb_ax.text(
        .7,8.8,'New Cases',
        size=14,
        fontdict={
            'family':'helvetica'
        }
    )
    cb.set_ticklabels(boundaries)
        
    ax.set_title(
        f'COVID Cases in 2020 at Week {wk}',
        size=18,
        fontdict={'fontweight':'bold','family':'helvetica'}
    )

    plt.savefig(
        f'covid_maps/daily_week{wk}',
        dpi=500,
        bbox_inches='tight'
    )
    
    plt.clf()

sc.stop()