import os
import psycopg2
from datetime import datetime
import pandas as pd
import lightgbm
import matplotlib.pyplot as plt
import numpy as np


DB_URL = 'postgresql://neondb_owner:SkYV5t7DpHoi@ep-long-grass-a57cixig.us-east-2.aws.neon.tech/neondb?sslmode=require'

def create_energy_insert_statement(temperature, humidity, square_footage, renewable_energy, energy_consumption, timestamp):
    
    sql = """
        INSERT INTO distributeddata (
            Timestamp,
            Temperature,
            Humidity,
            SquareFootage,
            RenewableEnergy,
            EnergyConsumption
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    # Assume the values are directly provided
    params = (
        timestamp,
        float(temperature),   
        float(humidity),      
        float(square_footage),  
        float(renewable_energy),  
        float(energy_consumption)  
    )
    
    return sql, params


def insert_energy_data(database_url, temperature, humidity, square_footage, renewable_energy, energy_consumption, timestamp):

    try:
        conn = psycopg2.connect(database_url)
        with conn.cursor() as cur:
            sql, params = create_energy_insert_statement(temperature, humidity, square_footage, renewable_energy, energy_consumption, timestamp)
            cur.execute(sql, params)
        conn.commit()
        print("Data inserted successfully")
    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        conn.close()


def get_latest_data_from_neon(database_url, window_size=7):
    query = """
        WITH HourlyData AS (
            SELECT 
                date_trunc('hour', timestamp) AS hour_bucket,
                AVG(temperature) AS avg_temperature,
                AVG(humidity) AS avg_humidity,
                AVG(squarefootage) AS avg_squarefootage,
                AVG(renewableenergy) AS avg_renewableenergy,
                AVG(energyconsumption) AS avg_energyconsumption
            FROM DistributedData
            GROUP BY hour_bucket
            ORDER BY hour_bucket DESC
            LIMIT 7
        )
        SELECT * 
        FROM HourlyData
        ORDER BY hour_bucket ASC;
    """

    try:
        # Connect to the database
        conn = psycopg2.connect(database_url)
        with conn.cursor() as cur:
            # Execute query
            cur.execute(query)
            columns = ['Timestamp', 'Temperature', 'Humidity', 'SquareFootage',
                       'RenewableEnergy', 'EnergyConsumption']

            # Fetch data and create DataFrame
            data = cur.fetchall()
            df = pd.DataFrame(data, columns=columns)
            df = df.sort_values('Timestamp')
            return df

    except Exception as e:
        print(f"Error retrieving data: {e}")
        return None
    finally:
        # Close the connection
        if 'conn' in locals():
            conn.close()

def prepare_neon_data_for_prediction(df, n_features=5):
    # Extract features in the same order as training
    features = df[['Temperature', 'Humidity', 'SquareFootage', 
                  'RenewableEnergy', 'EnergyConsumption']].values
    

    # Reshape for Conv1D: (samples, time steps, features)
    prediction_data = features.reshape(1, len(df), n_features)
    prediction_data = prediction_data.reshape(prediction_data.shape[0], -1)
    return prediction_data
    
def format_predictions_to_json(predictions):
    predictions = predictions.flatten()
    
    # Create formatted JSON
    prediction_json = {
        f"Next_{i+1}_day": float(predictions[i])
        for i in range(7)
    }
    
    return prediction_json

def plot_energy_forecast(historical_data, predicted_data):
    # Ensure historical data is flattened
    historical_data = historical_data.flatten()
    
    # Create continuous day range
    days = np.arange(1, 15)  # 14 days total
    
    # Create figure and axis
    plt.figure(figsize=(12, 6))
    
    # Plot historical data (first 7 days)
    plt.plot(days[:7], historical_data, color='blue', linewidth=2, 
             label='Historical Data')
    
    # Plot predicted data (next 7 days)
    plt.plot(days[6:], np.concatenate(([historical_data[-1]], predicted_data)), 
             color='orange', linewidth=2, label='Predicted')
    
    # Add vertical line to separate historical and predicted data
    plt.axvline(x=7, color='gray', linestyle='--', alpha=0.5)
    
    # Customize the plot
    plt.title('Energy Consumption: Historical vs Predicted', fontsize=14, pad=15)
    plt.xlabel('Days', fontsize=12)
    plt.ylabel('Energy Consumption', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.legend()
    
    # Set x-axis ticks to show all days
    plt.xticks(days)
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    return plt


def implement_prediction(db_url, window_size=7, n_features=5):
    # Load models
    models = []
    for i in range(7):
        model = lightgbm.Booster(model_file=f'lgbm_model_step_{i+1}.txt')
        models.append(model)

    # Retrieve latest data
    latest_data = get_latest_data_from_neon(db_url, window_size=window_size)

    if latest_data is not None and len(latest_data) == window_size:
        # Prepare data for prediction
        prediction_input = prepare_neon_data_for_prediction(
            df=latest_data,
            n_features=n_features
        )

        # Make predictions
        predictions_per_step = []
        for step in range(7):
            predictions = models[step].predict(prediction_input.reshape(prediction_input.shape[0], -1))
            predictions_per_step.append(predictions)

        # Combine all predictions into a single array
        predictions = np.array(predictions_per_step).T  # Shape: (n_samples, n_output_steps)

        print(predictions.shape)

        # Log the results
        print("Data retrieved from Neon DB:")
        print(latest_data)
        print("\nPrediction input shape:", prediction_input.shape)
        print("Prediction output shape:", predictions.shape)

        # Format predictions into JSON
        prediction_json = format_predictions_to_json(predictions)

        # Plot the results
        historical_data = latest_data['EnergyConsumption'].values  # Ensure this gives correct shape
        predicted_data = predictions.flatten()

        plt = plot_energy_forecast(historical_data, predicted_data)
        plt.show()

        return prediction_json
    else:
        print("Could not retrieve enough data for prediction")
        return None

if __name__ == "__main__":
    prediction_json = implement_prediction(DB_URL)

    if prediction_json:
        print("Prediction JSON:")
        print(prediction_json)
    else:
        print("Prediction pipeline failed to generate results.")
