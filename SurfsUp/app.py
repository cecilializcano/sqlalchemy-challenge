# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt
import sqlalchemy
from flask import Flask, jsonify
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy import create_engine, text, inspect, func
from datetime import datetime
from dateutil.relativedelta import relativedelta



#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
Base.prepare(autoload_with=engine)
connection=engine.connect()

# reflect the tables
hawaii_measurement_df=pd.read_sql('''SELECT * FROM measurement''',connection)
hawaii_station_df=pd.read_sql('''SELECT * FROM station''',connection)

# Save references to each table
measurement = Base.classes.measurement
station=Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def home():
    print("Server received request for 'Home' page...")
    return """Welcome to my 'Home' page!<br/>
            /api/v1.0/precipitation<br/>
            /api/v1.0/stations<br/>
            /api/v1.0/tobs<br/>
            /api/v1.0/start_date<br/>
            /api/v1.0/start_date/end_date<br/>
            Note 1 = replace start and end dates with dates following format YYYY-mm-dd<br/>
            Note 2 = To separate between start date and end date add a "/" """

@app.route("/api/v1.0/precipitation")
def precipitation():
    print("Welcome to my 'Precipitation' page!")
    # Save references to each table
    measurement = Base.classes.measurement
    station=Base.classes.station

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Design a query to retrieve the last 12 months of precipitation data and plot the results. 
    # Starting from the most recent data point in the database. 
    query_maxdate=session.query(func.max(measurement.date)).first()
    # Calculate the date one year from the last date in data set.
    maxdate = datetime.strptime(query_maxdate[0], '%Y-%m-%d')
    year_before_temp=maxdate-relativedelta(years=1)
    year_before=year_before_temp.strftime('%Y-%m-%d')

    # Perform a query to retrieve the data and precipitation scores
    results=session.query(measurement.date,measurement.prcp).\
        filter(measurement.date>=year_before).all()
    measurement_date_list=[]
    measurement_prcp_list=[]
    results_dict={}
    for result in results:
        (measurement_date,measurement_prcp)=result
        measurement_date_list.append(measurement_date)
        measurement_prcp_list.append(measurement_prcp)
        results_dict[measurement_date]=measurement_prcp
    session.close()
    return jsonify(results_dict)

@app.route("/api/v1.0/stations")
def stations():
    print("Welcome to my 'Station' page!")
    # Save references to each table
    measurement = Base.classes.measurement
    station=Base.classes.station

    # Create our session (link) from Python to the DB
    session = Session(engine)

    stations2=session.query(station.station,station.name).all()
    stations_dict={}
    for station in stations2:
        (station_station,station_name)=station
        stations_dict[station_station]=station_name
    
    session.close()

    return jsonify(stations_dict)

@app.route("/api/v1.0/tobs")
def tobs():
    print("Welcome to my 'tobs' page!")
    # Save references to each table
    measurement = Base.classes.measurement
    station=Base.classes.station

    # Create our session (link) from Python to the DB
    session = Session(engine)
    # Using the most active station id
    # Query the last 12 months of temperature observation data for this station and plot the results as a histogram
    query_maxdate=session.query(func.max(measurement.date)).first()
    # Calculate the date one year from the last date in data set.
    maxdate = datetime.strptime(query_maxdate[0], '%Y-%m-%d')
    year_before_temp=maxdate-relativedelta(years=1)
    year_before=year_before_temp.strftime('%Y-%m-%d')
    
    # Design a query to find the most active stations (i.e. which stations have the most rows?)
    # List the stations and their counts in descending order.
    most_active_stations=session.query(measurement.station,func.count(measurement.station)).\
        group_by(measurement.station).order_by(func.count(measurement.station).desc()).all()
        
    active_station=most_active_stations[0][0]
    temperatures_active_station_12months=session.query(measurement.date,measurement.tobs).\
        filter(measurement.date>=year_before).filter(measurement.station==active_station).all()

    measurement_date_list2=[]
    measurement_tobs_list=[]
    results_dict2={}
    for date_temperatures in temperatures_active_station_12months:
        (measurement_date2,measurement_tobs)=date_temperatures
        measurement_date_list2.append(measurement_date2)
        measurement_tobs_list.append(measurement_tobs)
        results_dict2[measurement_date2]=measurement_tobs

    session.close()

    return jsonify(results_dict2)

@app.route("/api/v1.0/<start>")
def temperatures_start(start):
    print("Welcome to my 'Start date' page!")
    # Save references to each table
    measurement = Base.classes.measurement
    station=Base.classes.station

    # Create our session (link) from Python to the DB
    session = Session(engine)

    query_enddate=session.query(func.max(measurement.date)).first()
    enddate=query_enddate[0]

    temperature_max=session.query(measurement.date,func.max(measurement.tobs)).\
        filter(measurement.date>=start).all()

    temperature_min=session.query(measurement.date,func.min(measurement.tobs)).\
        filter(measurement.date>=start).all()

    temperature_avg=session.query(measurement.date,func.avg(measurement.tobs)).\
        filter(measurement.date>=start).all()

    result_dict4={}

    for temperature in temperature_min:
        (date,temp)=temperature
        result_dict4['TMIN']=temp
    for temperature in temperature_max:
        (date,temp)=temperature
        result_dict4['TMAX']=temp
    for temperature in temperature_avg:
        (date,temp)=temperature
        result_dict4['TAVG']=temp

    session.close()

    return jsonify(result_dict4)


@app.route("/api/v1.0/<start>/<end>")
def temperatures_start_end(start,end):
    print("Welcome to my 'Start and end date' page!")
    # Save references to each table
    measurement = Base.classes.measurement
    station=Base.classes.station

    # Create our session (link) from Python to the DB
    session = Session(engine)

    
    enddate=end

    temperature_max=session.query(measurement.date,func.max(measurement.tobs)).\
        filter(measurement.date>=start).\
            filter(measurement.date<=end).all()

    temperature_min=session.query(measurement.date,func.min(measurement.tobs)).\
        filter(measurement.date>=start).\
            filter(measurement.date<=end).all()

    temperature_avg=session.query(measurement.date,func.avg(measurement.tobs)).\
        filter(measurement.date>=start).\
            filter(measurement.date<=end).all()

    result_dict5={}

    for temperature in temperature_min:
        (date,temp)=temperature
        result_dict5['TMIN']=temp
    for temperature in temperature_max:
        (date,temp)=temperature
        result_dict5['TMAX']=temp
    for temperature in temperature_avg:
        (date,temp)=temperature
        result_dict5['TAVG']=temp

    session.close()

    return jsonify(result_dict5)



if __name__ == "__main__":
    app.run(debug=True)