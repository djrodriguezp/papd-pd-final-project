from sqlalchemy import create_engine
import streamlit as st
import pandas as pd
from datetime import datetime


@st.cache(allow_output_mutation=True)
def load_covid_dataset():
    db_source = 'mysql+mysqlconnector://covid19:secretpass@127.0.0.1/covid19'
    db_conn = create_engine(db_source)
    df = pd.read_sql("SELECT * FROM global_data", con=db_conn)
    df["date"] = pd.to_datetime(df['date'], format='%Y-%m-%d').dt.date
    df = df.sort_values(['date'])
    return df


def filter_df(df, start_date, end_date, countries, states):
    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    if len(countries) > 0:
        mask &= (df['country'].isin(countries))

    if len(states) > 0:
        mask &= (df['state'].isin(states))

    return df[mask]


covid_df = load_covid_dataset()
min_date = covid_df['date'].min()
max_date =  covid_df['date'].max()
countries_list =pd.unique(covid_df['country'].dropna()).tolist()
states_list = pd.unique(covid_df['state'].dropna()).tolist()

st.title("Covid 19")

st.sidebar.title("Filtros")
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

#st.dataframe(covid_df)
#st.write(f"Usted ha elegido la fecha {end_date_filter} ")
filtered_data = filter_df(covid_df, start_date_filter, end_date_filter, countries_filter, states_filter)

filtered_data_map = filtered_data.dropna(subset=['lat','lon'])
st.map(filtered_data_map)