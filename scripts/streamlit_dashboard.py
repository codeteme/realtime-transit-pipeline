import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time

st.set_page_config(
    page_title="🚇 TfL Live Transit Dashboard",
    page_icon="🚇",
    layout="wide"
)

st.title("TfL Central Line - Live Transit Dashboard")
st.markdown("**Real-time train arrivals across London**")

# Auto-refresh
refresh_interval = st.sidebar.slider("Auto-refresh (seconds)", 5, 60, 10)

# Station coordinates for Central Line (major stations)
STATION_COORDS = {
    "West Ruislip Underground Station": {"lat": 51.5695, "lon": -0.4378},
    "Ruislip Gardens Underground Station": {"lat": 51.5608, "lon": -0.4103},
    "South Ruislip Underground Station": {"lat": 51.5569, "lon": -0.3988},
    "Northolt Underground Station": {"lat": 51.5483, "lon": -0.3687},
    "Greenford Underground Station": {"lat": 51.5423, "lon": -0.3456},
    "Perivale Underground Station": {"lat": 51.5367, "lon": -0.3233},
    "Hanger Lane Underground Station": {"lat": 51.5302, "lon": -0.2933},
    "North Acton Underground Station": {"lat": 51.5238, "lon": -0.2597},
    "East Acton Underground Station": {"lat": 51.5169, "lon": -0.2481},
    "White City Underground Station": {"lat": 51.5120, "lon": -0.2239},
    "Shepherd's Bush Underground Station": {"lat": 51.5048, "lon": -0.2188},
    "Holland Park Underground Station": {"lat": 51.5075, "lon": -0.2067},
    "Notting Hill Gate Underground Station": {"lat": 51.5094, "lon": -0.1967},
    "Queensway Underground Station": {"lat": 51.5108, "lon": -0.1876},
    "Lancaster Gate Underground Station": {"lat": 51.5119, "lon": -0.1756},
    "Marble Arch Underground Station": {"lat": 51.5136, "lon": -0.1586},
    "Bond Street Underground Station": {"lat": 51.5142, "lon": -0.1494},
    "Oxford Circus Underground Station": {"lat": 51.5152, "lon": -0.1419},
    "Tottenham Court Road Underground Station": {"lat": 51.5165, "lon": -0.1308},
    "Holborn Underground Station": {"lat": 51.5174, "lon": -0.1200},
    "Chancery Lane Underground Station": {"lat": 51.5185, "lon": -0.1114},
    "St. Paul's Underground Station": {"lat": 51.5148, "lon": -0.0978},
    "Bank Underground Station": {"lat": 51.5133, "lon": -0.0886},
    "Liverpool Street Underground Station": {"lat": 51.5178, "lon": -0.0823},
    "Bethnal Green Underground Station": {"lat": 51.5270, "lon": -0.0549},
    "Mile End Underground Station": {"lat": 51.5249, "lon": -0.0332},
    "Stratford Underground Station": {"lat": 51.5416, "lon": -0.0042},
    "Leyton Underground Station": {"lat": 51.5566, "lon": 0.0053},
    "Leytonstone Underground Station": {"lat": 51.5681, "lon": 0.0083},
    "Snaresbrook Underground Station": {"lat": 51.5803, "lon": 0.0214},
    "South Woodford Underground Station": {"lat": 51.5917, "lon": 0.0271},
    "Woodford Underground Station": {"lat": 51.6071, "lon": 0.0340},
    "Buckhurst Hill Underground Station": {"lat": 51.6266, "lon": 0.0464},
    "Loughton Underground Station": {"lat": 51.6412, "lon": 0.0756},
    "Debden Underground Station": {"lat": 51.6456, "lon": 0.0838},
    "Theydon Bois Underground Station": {"lat": 51.6722, "lon": 0.1031},
    "Epping Underground Station": {"lat": 51.6937, "lon": 0.1139},
    "Wanstead Underground Station": {"lat": 51.5772, "lon": 0.0289},
    "Redbridge Underground Station": {"lat": 51.5760, "lon": 0.0454},
    "Gants Hill Underground Station": {"lat": 51.5765, "lon": 0.0661},
    "Newbury Park Underground Station": {"lat": 51.5752, "lon": 0.0900},
    "Barkingside Underground Station": {"lat": 51.5856, "lon": 0.0886},
    "Fairlop Underground Station": {"lat": 51.5960, "lon": 0.0910},
    "Hainault Underground Station": {"lat": 51.6030, "lon": 0.0931},
    "Grange Hill Underground Station": {"lat": 51.6127, "lon": 0.0976},
    "Chigwell Underground Station": {"lat": 51.6177, "lon": 0.0755},
    "Roding Valley Underground Station": {"lat": 51.6174, "lon": 0.0439},
    "Ealing Broadway Underground Station": {"lat": 51.5152, "lon": -0.3017},
}

@st.cache_data(ttl=refresh_interval)
def load_data():
    try:
        df = pd.read_parquet('data/gold')
        df['minutes'] = df['seconds'] / 60.0
        
        # Add coordinates
        df['lat'] = df['station'].map(lambda x: STATION_COORDS.get(x, {}).get('lat'))
        df['lon'] = df['station'].map(lambda x: STATION_COORDS.get(x, {}).get('lon'))
        
        return df
    except Exception as e:
        st.error(f"Error: {e}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("No data yet. Run the pipeline first!")
    st.stop()

st.sidebar.markdown(f"**Updated:** {datetime.now().strftime('%H:%M:%S')}")
st.sidebar.markdown(f"**Records:** {len(df):,}")

# Metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("Avg Wait", f"{df['minutes'].mean():.1f} min")
col2.metric("Arrivals", f"{len(df)}")
col3.metric("Stations", f"{df['station'].nunique()}")
col4.metric("Next Train", f"{df['minutes'].min():.1f} min")

st.markdown("---")

# MAP - Full width
st.subheader("Live Train Positions Across London")

# Prepare map data
map_df = df.dropna(subset=['lat', 'lon']).copy()
map_df['size'] = 50 - (map_df['minutes'] * 2)  # Bigger = arriving sooner
map_df['size'] = map_df['size'].clip(10, 50)
map_df['color'] = map_df['minutes'].apply(
    lambda x: 'red' if x < 2 else 'orange' if x < 5 else 'green'
)

if not map_df.empty:
    # Create map
    fig_map = go.Figure()
    
    # Add train markers
    for color, label in [('red', '< 2 min'), ('orange', '2-5 min'), ('green', '> 5 min')]:
        subset = map_df[map_df['color'] == color]
        if not subset.empty:
            fig_map.add_trace(go.Scattermapbox(
                lat=subset['lat'],
                lon=subset['lon'],
                mode='markers',
                marker=dict(size=subset['size'], color=color, opacity=0.7),
                text=subset['station'] + '<br>' + subset['minutes'].round(1).astype(str) + ' min',
                hoverinfo='text',
                name=label
            ))
    
    # Map layout
    fig_map.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=51.5155, lon=-0.0922),  # Central London
            zoom=10
        ),
        height=500,
        margin=dict(l=0, r=0, t=0, b=0),
        showlegend=True,
        legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
    )
    
    st.plotly_chart(fig_map, use_container_width=True)
    
    # Station heatmap
    st.subheader("Station Activity Heatmap")
    station_counts = map_df.groupby(['station', 'lat', 'lon']).size().reset_index(name='count')
    
    fig_heat = go.Figure(go.Densitymapbox(
        lat=station_counts['lat'],
        lon=station_counts['lon'],
        z=station_counts['count'],
        radius=20,
        colorscale='Hot',
        showscale=True,
        hovertext=station_counts['station'] + '<br>Arrivals: ' + station_counts['count'].astype(str),
        hoverinfo='text'
    ))
    
    fig_heat.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=51.5155, lon=-0.0922),
            zoom=10
        ),
        height=400,
        margin=dict(l=0, r=0, t=0, b=0)
    )
    
    st.plotly_chart(fig_heat, use_container_width=True)
else:
    st.warning("No location data available for mapping")

st.markdown("---")

# Charts
col1, col2 = st.columns(2)

with col1:
    st.subheader("Wait Time Distribution")
    fig = px.histogram(df, x='minutes', nbins=30, 
                      labels={'minutes': 'Minutes to Arrival'},
                      color_discrete_sequence=['#0019A8'])
    fig.update_layout(showlegend=False, height=300)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("🚉 Top 10 Busiest Stations")
    top = df['station'].value_counts().head(10)
    fig = px.bar(y=top.index, x=top.values, orientation='h',
                labels={'x': 'Arrivals', 'y': 'Station'},
                color=top.values, color_continuous_scale='Blues')
    fig.update_layout(showlegend=False, height=300)
    st.plotly_chart(fig, use_container_width=True)

# Data tables
col1, col2 = st.columns(2)

with col1:
    st.subheader("Next 10 Arrivals")
    next_10 = df.nsmallest(10, 'minutes')[['station', 'line_name', 'minutes']]
    next_10['minutes'] = next_10['minutes'].round(1)
    st.dataframe(next_10.reset_index(drop=True), use_container_width=True, height=300)

with col2:
    st.subheader("Average Wait by Station")
    avg_wait = df.groupby('station')['minutes'].mean().sort_values().head(10)
    avg_df = pd.DataFrame({
        'Station': avg_wait.index,
        'Avg Minutes': avg_wait.values.round(1)
    })
    st.dataframe(avg_df.reset_index(drop=True), use_container_width=True, height=300)

# Auto-refresh
time.sleep(refresh_interval)
st.rerun()
