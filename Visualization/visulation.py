import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
from datetime import datetime
import folium
from folium.plugins import HeatMap, MarkerCluster
import plotly.graph_objects as go
import plotly.express as px
from matplotlib.animation import FuncAnimation, PillowWriter
import warnings
warnings.filterwarnings('ignore')

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "dublin_bus_db",
    "user": "dap",
    "password": "dap"
}
OUTPUT_DIR = "visualizations"
VIDEO_DIR = "videos"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(VIDEO_DIR, exist_ok=True)

plt.style.use('dark_background')
sns.set_palette("husl")

def get_data():
    """Fetch data from DB"""
    conn = psycopg2.connect(**DB_CONFIG)
    print(" Fetching comprehensive data...")
    
    query = """
        SELECT 
            arrival_delay, 
            temperature, 
            wind_speed, 
            humidity,
            vehicle_timestamp,
            route_id,
            latitude,
            longitude
        FROM bus_weather_merged 
        WHERE arrival_delay IS NOT NULL 
          AND vehicle_timestamp IS NOT NULL
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        ORDER BY RANDOM() 
        LIMIT 100000
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f" Loaded {len(df)} rows with geo-data.")
    
    df['vehicle_timestamp'] = pd.to_datetime(df['vehicle_timestamp'])
    df['hour'] = df['vehicle_timestamp'].dt.hour
    df['day_of_week'] = df['vehicle_timestamp'].dt.day_name()
    df['date'] = df['vehicle_timestamp'].dt.date
    
    df_filtered = df[
        (df['arrival_delay'] > -600) & 
        (df['arrival_delay'] < 3600)
    ].copy()
    
    print(f"Filtered to {len(df_filtered)} rows (removed outliers).")
    return df_filtered


def create_geo_heatmap(df):
    """Create interactive heatmap showing delay hotspots across Dublin"""
    print("Creating geo heatmap...")
    
    dublin_center = [53.3498, -6.2603]
    m = folium.Map(
        location=dublin_center,
        zoom_start=12,
        tiles='OpenStreetMap'
    )
    
    high_delay_data = df[df['arrival_delay'] > 300][['latitude', 'longitude', 'arrival_delay']]
    heat_data = [[row['latitude'], row['longitude'], row['arrival_delay']] 
                 for idx, row in high_delay_data.iterrows()]
    
    HeatMap(heat_data, radius=15, blur=25, max_zoom=1, min_opacity=0.3).add_to(m)
    
    m.save(f'{OUTPUT_DIR}/05_geo_heatmap.html')
    print("Generated: 05_geo_heatmap.html")

def create_delay_hotspots_map(df):
    """Create marker cluster map showing routes with delays"""
    print(" Creating delay hotspots map")
    
    dublin_center = [53.3498, -6.2603]
    m = folium.Map(location=dublin_center, zoom_start=12, tiles='OpenStreetMap')
    
    high_delay = df[df['arrival_delay'] > 200].head(500)
    
    for idx, row in high_delay.iterrows():
        color = 'red' if row['arrival_delay'] > 500 else 'orange' if row['arrival_delay'] > 300 else 'yellow'
        
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=5,
            popup=f"Route: {row['route_id']}<br>Delay: {row['arrival_delay']:.0f}s<br>Temp: {row['temperature']:.1f}°C",
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.7
        ).add_to(m)
    
    m.save(f'{OUTPUT_DIR}/06_delay_hotspots_map.html')
    print("Generated: 06_delay_hotspots_map.html")



def plot_2d_scatter_temperature_delay(df):
    """2D Scatter: Temperature vs Delay (colored by humidity, sized by wind)"""
    print("Creating 2D scatter: Temperature vs Delay...")
    
    df_clean = df.dropna(subset=['wind_speed', 'humidity']).copy()
    print(f"   Using {len(df_clean)} rows (removed {len(df) - len(df_clean)} NaN values)")
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df_clean['temperature'],
        y=df_clean['arrival_delay'],
        mode='markers',
        marker=dict(
            size=df_clean['wind_speed'] * 2 + 2,
            color=df_clean['humidity'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="Humidity %"),
            line=dict(width=0.5, color='white')
        ),
        text=[f"Route: {r}<br>Temp: {t:.1f}°C<br>Delay: {d:.0f}s<br>Wind: {w:.1f}" 
              for r, t, d, w in zip(df_clean['route_id'], df_clean['temperature'], df_clean['arrival_delay'], df_clean['wind_speed'])],
        hovertemplate='%{text}<extra></extra>',
        name='Delays'
    ))
    
    fig.update_layout(
        title='Relationship: Temperature vs Arrival Delay (colored by Humidity)',
        xaxis_title='Temperature (°C)',
        yaxis_title='Arrival Delay (seconds)',
        width=1000,
        height=600,
        hovermode='closest',
        template='plotly_dark'
    )
    
    fig.write_html(f'{OUTPUT_DIR}/07_scatter_temp_delay.html')
    print(" Generated: 07_scatter_temp_delay.html")

def plot_2d_scatter_wind_humidity(df):
    """2D Scatter: Wind Speed vs Humidity (colored by delay)"""
    print("Creating 2D scatter: Wind vs Humidity...")
    
    df_clean = df.dropna(subset=['wind_speed', 'humidity']).copy()
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df_clean['wind_speed'],
        y=df_clean['humidity'],
        mode='markers',
        marker=dict(
            size=6,
            color=df_clean['arrival_delay'],
            colorscale='RdYlGn_r',
            showscale=True,
            colorbar=dict(title="Delay (s)"),
            opacity=0.7,
            line=dict(width=0.5, color='white')
        ),
        text=[f"Route: {r}<br>Wind: {w:.1f} m/s<br>Humidity: {h:.1f}%<br>Delay: {d:.0f}s" 
              for r, w, h, d in zip(df_clean['route_id'], df_clean['wind_speed'], df_clean['humidity'], df_clean['arrival_delay'])],
        hovertemplate='%{text}<extra></extra>',
        name='Observations'
    ))
    
    fig.update_layout(
        title='Weather Correlation: Wind Speed vs Humidity (colored by Delay)',
        xaxis_title='Wind Speed (m/s)',
        yaxis_title='Humidity (%)',
        width=1000,
        height=600,
        hovermode='closest',
        template='plotly_dark'
    )
    
    fig.write_html(f'{OUTPUT_DIR}/08_scatter_wind_humidity.html')
    print(" Generated: 08_scatter_wind_humidity.html")


def plot_3d_scatter_temperature_wind_delay(df):
    """3D Scatter: Temperature, Wind Speed, Delay"""
    print(" Creating 3D scatter: Temperature x Wind x Delay")
    
    df_clean = df.dropna(subset=['wind_speed', 'humidity']).copy()
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter3d(
        x=df_clean['temperature'],
        y=df_clean['wind_speed'],
        z=df_clean['arrival_delay'],
        mode='markers',
        marker=dict(
            size=3,
            color=df_clean['humidity'],
            colorscale='Turbo',
            showscale=True,
            colorbar=dict(title="Humidity %"),
            opacity=0.8
        ),
        text=[f"Temp: {t:.1f}°C<br>Wind: {w:.1f} m/s<br>Delay: {d:.0f}s<br>Route: {r}" 
              for t, w, d, r in zip(df_clean['temperature'], df_clean['wind_speed'], df_clean['arrival_delay'], df_clean['route_id'])],
        hovertemplate='%{text}<extra></extra>',
        name='Observations'
    ))
    
    fig.update_layout(
        title='3D Analysis: Temperature × Wind Speed × Arrival Delay',
        scene=dict(
            xaxis_title='Temperature (°C)',
            yaxis_title='Wind Speed (m/s)',
            zaxis_title='Delay (seconds)',
            camera=dict(eye=dict(x=1.5, y=1.5, z=1.3))
        ),
        width=1000,
        height=700,
        template='plotly_dark'
    )
    
    fig.write_html(f'{OUTPUT_DIR}/09_scatter_3d_temp_wind_delay.html')
    print(" Generated: 09_scatter_3d_temp_wind_delay.html")

def plot_3d_scatter_geo_delay(df):
    """3D Scatter: Latitude, Longitude, Delay"""
    print(" Creating 3D geo-scatter: Lat x Lon x Delay...")
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter3d(
        x=df['longitude'],
        y=df['latitude'],
        z=df['arrival_delay'],
        mode='markers',
        marker=dict(
            size=3,
            color=df['arrival_delay'],
            colorscale='RdYlGn_r',
            showscale=True,
            colorbar=dict(title="Delay (s)"),
            opacity=0.7
        ),
        text=[f"Location: ({lat:.4f}, {lon:.4f})<br>Delay: {d:.0f}s<br>Route: {r}" 
              for lat, lon, d, r in zip(df['latitude'], df['longitude'], df['arrival_delay'], df['route_id'])],
        hovertemplate='%{text}<extra></extra>',
        name='Routes'
    ))
    
    fig.update_layout(
        title='3D Geo-Spatial: Dublin Routes Vs Arrival Delays',
        scene=dict(
            xaxis_title='Longitude',
            yaxis_title='Latitude',
            zaxis_title='Delay (seconds)',
            camera=dict(eye=dict(x=1, y=1, z=1.3))
        ),
        width=1000,
        height=700,
        template='plotly_dark'
    )
    
    fig.write_html(f'{OUTPUT_DIR}/10_scatter_3d_geo_delay.html')
    print("Generated: 10_scatter_3d_geo_delay.html")


def plot_violin_delay_by_hour(df):
    """Violin plot: Distribution of delays by hour"""
    print("Creating violin plot: Delay distribution by hour...")
    
    fig = px.violin(
        df,
        x='hour',
        y='arrival_delay',
        points='outliers',
        title='Distribution of Delays Across Hours (Violin Plot)',
        labels={'hour': 'Hour of Day', 'arrival_delay': 'Delay (seconds)'},
        color='hour'
    )
    
    fig.update_layout(width=1000, height=600, template='plotly_dark')
    fig.write_html(f'{OUTPUT_DIR}/11_violin_delay_by_hour.html')
    print("Generated: 11_violin_delay_by_hour.html")

def plot_density_heatmap_geo(df):
    """2D Density heatmap of delays across Dublin"""
    print("Creating 2D density heatmap: Geographic distribution...")
    
    fig = go.Figure()
    
    fig.add_trace(go.Histogram2dContour(
        x=df['longitude'],
        y=df['latitude'],
        z=df['arrival_delay'],
        colorscale='Viridis',
        colorbar=dict(title="Delay (s)"),
        showscale=True
    ))
    
    fig.update_layout(
        title='2D Density Heatmap: Delay Hotspots Across Dublin',
        xaxis_title='Longitude',
        yaxis_title='Latitude',
        width=1000,
        height=600,
        template='plotly_dark'
    )
    
    fig.write_html(f'{OUTPUT_DIR}/12_density_heatmap_geo.html')
    print("Generated: 12_density_heatmap_geo.html")


def create_animated_video_hourly_changes(df):
    """Create animated GIF showing how delays change across 24 hours"""
    print("🎬 Creating animated video: Hourly delay changes...")
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    hours = sorted(df['hour'].unique())
    
    def update_frame(hour):
        ax1.clear()
        ax2.clear()
        
        hour_data = df[df['hour'] == hour]
        
        ax1.hist(hour_data['arrival_delay'], bins=30, color='skyblue', edgecolor='black', alpha=0.7)
        ax1.axvline(hour_data['arrival_delay'].mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {hour_data["arrival_delay"].mean():.0f}s')
        ax1.set_title(f'Hour {hour:02d}:00 - Delay Distribution', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Delay (seconds)')
        ax1.set_ylabel('Frequency')
        ax1.legend()
        ax1.set_xlim(-600, 2000)
        
        scatter = ax2.scatter(hour_data['temperature'], hour_data['arrival_delay'], 
                             c=hour_data['wind_speed'], cmap='viridis', s=50, alpha=0.6)
        ax2.set_title(f'Temperature vs Delay at Hour {hour:02d}:00', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Temperature (°C)')
        ax2.set_ylabel('Delay (seconds)')
        ax2.set_ylim(-600, 2000)
        cbar = plt.colorbar(scatter, ax=ax2)
        cbar.set_label('Wind Speed')
        
        plt.suptitle(f'Dublin Bus Delays - Hour {hour:02d}:00 | {len(hour_data)} observations', 
                     fontsize=16, fontweight='bold', y=1.00)
        plt.tight_layout()
    
    anim = FuncAnimation(fig, update_frame, frames=hours, interval=500, repeat=True)
    writer = PillowWriter(fps=2)
    anim.save(f'{VIDEO_DIR}/delay_changes_hourly.gif', writer=writer)
    print(f"Generated: {VIDEO_DIR}/delay_changes_hourly.gif")
    plt.close()

def create_animated_video_daily_changes(df):
    """Create animated GIF showing how delays change across days of week"""
    print("Creating animated video: Daily delay changes")
    
    fig, ax = plt.subplots(figsize=(12, 7))
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    def update_frame(day):
        ax.clear()
        
        day_data = df[df['day_of_week'] == day]
        
        if len(day_data) == 0:
            ax.text(0.5, 0.5, f'No data for {day}', ha='center', va='center', fontsize=14)
            return
        
        scatter = ax.scatter(day_data['temperature'], day_data['arrival_delay'],
                            c=day_data['hour'], cmap='twilight', s=30, alpha=0.6, edgecolors='white', linewidth=0.5)
        ax.axhline(0, color='red', linestyle='--', linewidth=1, alpha=0.5, label='On Time')
        ax.set_title(f'{day} - Temperature vs Delay', fontsize=14, fontweight='bold')
        ax.set_xlabel('Temperature (°C)', fontsize=12)
        ax.set_ylabel('Delay (seconds)', fontsize=12)
        ax.set_ylim(-600, 2000)
        ax.grid(True, alpha=0.3)
        
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('Hour of Day')
        
        stats_text = f'Mean Delay: {day_data["arrival_delay"].mean():.0f}s\nStd Dev: {day_data["arrival_delay"].std():.0f}s\nSamples: {len(day_data)}'
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, fontsize=10,
               verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
        
        plt.tight_layout()
    
    anim = FuncAnimation(fig, update_frame, frames=days_order, interval=800, repeat=True)
    writer = PillowWriter(fps=1.5)
    anim.save(f'{VIDEO_DIR}/delay_changes_daily.gif', writer=writer)
    print(f"Generated: {VIDEO_DIR}/delay_changes_daily.gif")
    plt.close()


if __name__ == "__main__":
    print("ENHANCED VISUALIZATION SUITE: Dublin Bus Project (FULLY FIXED v3)")
    
    try:
        df = get_data()
        
        print("\n GEO-SPATIAL VISUALIZATIONS:")
        create_geo_heatmap(df)
        create_delay_hotspots_map(df)
        
        print("\n2D SCATTER PLOTS:")
        plot_2d_scatter_temperature_delay(df)
        plot_2d_scatter_wind_humidity(df)
        
        print("\n 3D SCATTER PLOTS:")
        plot_3d_scatter_temperature_wind_delay(df)
        plot_3d_scatter_geo_delay(df)
        
        print("\n ADVANCED BEAUTIFUL PLOTS:")
        plot_violin_delay_by_hour(df)
        plot_density_heatmap_geo(df)
        
        print("\n ANIMATED VIDEOS:")
        create_animated_video_hourly_changes(df)
        create_animated_video_daily_changes(df)
        
        print("ALL VISUALIZATIONS COMPLETED!")
        
        print(f"\nVisualizations saved to: {os.path.abspath(OUTPUT_DIR)}")
        print(f"Videos saved to: {os.path.abspath(VIDEO_DIR)}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()