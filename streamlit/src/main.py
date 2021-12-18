import pydeck as pdk
import numpy as np
from sqlalchemy import create_engine
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder

st.set_page_config(layout="wide")

@st.cache(allow_output_mutation=True)
def load_covid_dataset():
    #db_source = 'mysql+mysqlconnector://covid19:secretpass@127.0.0.1/covid19'
    db_source = 'mysql+mysqlconnector://covid19:secretpass@mysql_db/covid19'
    db_conn = create_engine(db_source)
    df = pd.read_sql("SELECT * FROM global_data", con=db_conn)
    df["date"] = pd.to_datetime(df['date'], format='%Y-%m-%d').dt.date
    df["state"] = df["state"].fillna('n/a')
    df['cases_int'] = df['cases']
    df["cases"] = df["cases_int"].map('{:,d}'.format)
    df = df.sort_values(['date'])

    return df


def filter_df(df, start_date, end_date, countries, states):
    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    if len(countries) > 0:
        mask &= (df['country'].isin(countries))

    if len(states) > 0:
        mask &= (df['state'].isin(states))

    return df[mask]


def get_cases_count(df, grouby, status=["confirmed", "recovered","deaths"]):
    # delta_df = df[(df['status'] == status)].groupby(['country', 'state', 'lat', 'lon'], dropna=True)['count'].agg(['first', 'last']).reset_index()
    # delta_df['count'] = delta_df['last'] - delta_df['first']
    # delta_df["count_str"] = delta_df["count"].map('{:,d}'.format)
    # delta_df.drop(columns=['first', 'last'], inplace=True)
    count_df = df[(df['status'].isin(status))].groupby(grouby).agg({'cases_int': 'sum'}).reset_index()
    count_df["cases"] = count_df["cases_int"].map('{:,d}'.format)
    return count_df[(count_df["cases_int"] >= 0)]


covid_df = load_covid_dataset()
min_date = covid_df['date'].min()
max_date =  covid_df['date'].max()
countries_list =pd.unique(covid_df['country'].dropna()).tolist()
states_list = pd.unique(covid_df['state'].dropna()).tolist()

st.title("Covid 19")

st.sidebar.title("Filtros")
status_filter = st.sidebar.selectbox(
    "Tipo de Casos",
    ('Confirmados', 'Recuperados', 'Muertes')
)
start_date_filter = st.sidebar.date_input(
    "Fecha Inicio",
    min_date,
    min_value=min_date,
    max_value=max_date
)

end_date_filter = st.sidebar.date_input(
    "Fecha Final",
    max_date,
    min_value=min_date,
    max_value=max_date
)
countries_filter = st.sidebar.multiselect(
    "País",
    tuple(countries_list)
)

states_filter = st.sidebar.multiselect(
    "Estado",
    tuple(states_list)
)




#st.write(f"Usted ha elegido la fecha {end_date_filter} ")
filtered_data = filter_df(covid_df, start_date_filter, end_date_filter, countries_filter, states_filter)


confirmed_cases_df = get_cases_count(filtered_data, ['country', 'state', 'lat', 'lon'], ["confirmed"])
#st.dataframe(confirmed_cases_df)
recovered_cases_df = get_cases_count(filtered_data, ['country', 'state', 'lat', 'lon'], ["recovered"])
death_cases_df = get_cases_count(filtered_data, ['country', 'state', 'lat', 'lon'], ["deaths"])

map_df = None
map_scatter_color = None
if status_filter == "Confirmados":
    map_df = confirmed_cases_df
    map_scatter_color = [255, 0, 0, 128]
    map_label = "Casos confirmados"
elif status_filter == "Recuperados":
    map_df = recovered_cases_df
    map_scatter_color = [0, 255, 0, 128]
    map_label = "Casos recuperados"
else:
    map_df = death_cases_df
    map_scatter_color = [255, 51, 255, 128]
    map_label = "Muertes"

map_df = pd.DataFrame(map_df.dropna(subset=['lat','lon']))

map_col1, map_col2 = st.columns([3, 1])

map_col1.pydeck_chart(pdk.Deck(
     map_style='dark',
     initial_view_state=pdk.ViewState(
         latitude=25,
         longitude=0,
         zoom=1
     ),
    layers=[
        pdk.Layer(
            'ScatterplotLayer',
            data=map_df,
            radius_scale=1,
            radius_min_pixels=3,
            radius_max_pixels=25,
            line_width_min_pixels=1,
            get_position=['lon', 'lat'],
            get_color=map_scatter_color,
            get_radius="cases_int",
            opacity=0.7,
            stroked=False,
            filled=True,
            pickable=True
        ),
    ],
    tooltip={
            "html": "<b>"+map_label+":</b> {cases}"
            "<br /><b>Country:</b> {country}"
            "<br/> <b>Region/State:</b> {state}",
            "style": {"color": "white"},
        },
 ))

# Operations to get total cases and delta from previous day
metric_total_cases = filtered_data[(filtered_data['status'] == 'confirmed')]['cases_int'].sum()
metric_delta_cases = metric_total_cases - filtered_data[(filtered_data['status'] == 'confirmed') & (filtered_data['date'] < end_date_filter)]['cases_int'].sum()
metric_total_deaths = filtered_data[(filtered_data['status'] == 'deaths')]['cases_int'].sum()
metric_delta_deaths = metric_total_deaths - filtered_data[(filtered_data['status'] == 'deaths') & (filtered_data['date'] < end_date_filter)]['cases_int'].sum()
metric_total_recovered = filtered_data[(filtered_data['status'] == 'recovered') & (filtered_data['cases_int'] >= 0) ]['cases_int'].sum()
metric_delta_recovered = metric_total_recovered - filtered_data[(filtered_data['status'] == 'recovered')
                                                                & (filtered_data['date'] < end_date_filter)
                                                                & (filtered_data['cases_int'] >= 0)]['cases_int'].sum()

map_col2.metric(
    "Total de Casos Confirmados",
    value = f"{metric_total_cases:,}",
    delta =  f"{metric_delta_cases:,} desde el dia anterior",
    delta_color = "inverse"
)

map_col2.metric(
    "Total de Muertes",
    value = f"{metric_total_deaths:,}",
    delta =  f"{metric_delta_deaths:,} desde el dia anterior",
    delta_color = "inverse"
)

map_col2.metric(
    "Total de Casos Recuperados",
    value=f"{metric_total_recovered:,}",
    delta=f"{metric_delta_recovered:,} desde el dia anterior"
)

gb = GridOptionsBuilder.from_dataframe(map_df)
gb.configure_pagination()
gridOptions = gb.build()

table_expander = st.expander("Ver tabla de datos mostrados en mapa")
with table_expander:
    AgGrid(map_df, gridOptions=gridOptions)

col1, col2, col3 = st.columns(3)
total_confirmed_by_date = get_cases_count(filtered_data, ['date', 'status'], ['confirmed'])
total_deaths_by_date = get_cases_count(filtered_data, ['date', 'status'], ['deaths'])
total_recovered_by_date = get_cases_count(filtered_data, ['date', 'status'], ['recovered'])

plot_df = pd.DataFrame(columns=['date','cases_int'])

with col1:
    fig = px.area(total_confirmed_by_date, x="date", y="cases_int",
                  labels={"date": "Fecha", "cases_int": "Casos Confirmados"},
                  title="Casos Totales Confirmados por Día",
                  color="status",
                  color_discrete_map={"confirmed": 'rgba(255,0,0,0.8)'},
                  width= 600)
    st.plotly_chart(fig)

with col2:
    fig = px.area(total_deaths_by_date, x="date", y="cases_int",
                  labels={"date": "Fecha", "cases_int": "Muertes"},
                  title="Muertes por Día",
                  color="status",
                  color_discrete_map={"deaths": 'rgba(255,51,255,0.8)' },
                  width= 600)
    st.plotly_chart(fig)

with col3:
    fig = px.area(total_recovered_by_date, x="date", y="cases_int",
                  labels={"date": "Fecha", "cases_int": "Casos Recuperados"},
                  title="Casos Recuperados Totales por Día",
                  color="status",
                  color_discrete_map={"recovered": 'rgba(0,255,0,0.8)'},
                  width= 600)
    st.plotly_chart(fig)