import pydeck as pdk
import numpy as np
from sqlalchemy import create_engine
import streamlit as st
import pandas as pd
from datetime import datetime

st.set_page_config(layout="wide")

@st.cache(allow_output_mutation=True)
def load_covid_dataset():
    db_source = 'mysql+mysqlconnector://covid19:secretpass@127.0.0.1/covid19'
    db_conn = create_engine(db_source)
    df = pd.read_sql("SELECT * FROM global_data", con=db_conn)
    df["date"] = pd.to_datetime(df['date'], format='%Y-%m-%d').dt.date
    df["state"] = df["state"].fillna('n/a')
    df = df.sort_values(['date'])
    return df


def filter_df(df, start_date, end_date, countries, states):
    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    if len(countries) > 0:
        mask &= (df['country'].isin(countries))

    if len(states) > 0:
        mask &= (df['state'].isin(states))

    return df[mask]


def get_cases_count(df, status):
    # delta_df = df[(df['status'] == status)].groupby(['country', 'state', 'lat', 'lon'], dropna=True)['count'].agg(['first', 'last']).reset_index()
    # delta_df['count'] = delta_df['last'] - delta_df['first']
    # delta_df["count_str"] = delta_df["count"].map('{:,d}'.format)
    # delta_df.drop(columns=['first', 'last'], inplace=True)
    count_df = df[(df['status'] == status)].groupby(['country', 'state', 'lat', 'lon'])['cases'].sum()
    return count_df

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


confirmed_cases_df = get_cases_count(filtered_data, "confirmed")
st.dataframe(confirmed_cases_df)
# recovered_cases_df = get_delta_count(filtered_data, "recovered")
# death_cases_df = get_delta_count(filtered_data, "deaths")
#
# map_df = None
# map_scatter_color = None
# if status_filter == "Confirmados":
#     map_df = confirmed_cases_df
#     map_scatter_color = [255, 0, 0, 128]
#     map_label = "Casos confirmados"
# elif status_filter == "Recuperados":
#     map_df = recovered_cases_df
#     map_scatter_color = [0, 255, 0, 128]
#     map_label = "Casos recuperados"
# else:
#     map_df = death_cases_df
#     map_scatter_color = [255, 51, 255, 128]
#     map_label = "Muertes"
#
# map_df = pd.DataFrame(map_df.dropna(subset=['lat','lon']))
#
# map_col1, map_col2 = st.columns([3, 1])
#
# map_col1.pydeck_chart(pdk.Deck(
#      map_style='dark',
#      initial_view_state=pdk.ViewState(
#          latitude=25,
#          longitude=0,
#          zoom=1
#      ),
#     layers=[
#         pdk.Layer(
#             'ScatterplotLayer',
#             data=map_df,
#             radius_scale=1,
#             radius_min_pixels=3,
#             radius_max_pixels=25,
#             line_width_min_pixels=1,
#             get_position=['lon', 'lat'],
#             get_color=map_scatter_color,
#             get_radius="count",
#             opacity=0.7,
#             stroked=False,
#             filled=True,
#             pickable=True
#         ),
#     ],
#     tooltip={
#             "html": "<b>"+map_label+":</b> {count_str}"
#             "<br /><b>Country:</b> {country}"
#             "<br/> <b>Region/State:</b> {state}",
#             "style": {"color": "white"},
#         },
#  ))
#
# map_col2.metric(
#     "Casos Confirmados",
#     value = 100
# )
#
# map_col2.metric(
#     "Casos Confirmados",
#     value = 50
# )
#
# table_expander = st.expander("Ver tabla de datos")
# table_expander.dataframe(filtered_data)
#
# col1, col2, col3 = st.columns(3)
#
# with col1:
#     st.header("Mi grafiquita 1")
#     st.image("https://static.streamlit.io/examples/cat.jpg")
#
# with col2:
#     st.header("Mi grafiquita 2")
#     st.image("https://static.streamlit.io/examples/dog.jpg")
#
# with col3:
#     st.header("Mi grafiquita 3")
#     st.image("https://static.streamlit.io/examples/owl.jpg")

#st.dataframe(filtered_data)