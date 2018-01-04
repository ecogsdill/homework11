#boilerplate code for using classes to interact with SQL databases
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base #convert python classes into SQL tables
from sqlalchemy.ext.automap import automap_base #convert python classes into SQL tables
from sqlalchemy import Column, Integer, String, Float, text
import pymysql
from sqlalchemy.orm import Session
pymysql.install_as_MySQLdb()
from sqlalchemy import func
from sqlalchemy import distinct
import matplotlib.pyplot as plt
import numpy as np
import datetime

engine=create_engine("sqlite:///../hawaii.sqlite")

session=Session(bind=engine)

#Declare a Base useing automap_base()

Base = automap_base()

Base.prepare(engine, reflect=True)

Stations = Base.classes.stations
Measurements = Base.classes.measurements

#################################################
# Flask Setup
#################################################

from flask import Flask, jsonify

app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/api/v1.0/stations")
def stations():
    stations_query = session.query(Stations.station)
    stations = pd.read_sql(stations_query.statement, stations_query.session.bind)
    return(jsonify(stations.to_dict()))

@app.route("/api/v1.0/tobs")
def tobs():
    p12m_temp_query = session.query(Measurements).filter(Measurements.date>='2016-08-01',Measurements.date<'2017-08-01').with_entities(Measurements.date, Measurements.station, Measurements.tobs)
    p12m_temp = pd.read_sql(p12m_temp_query.statement, p12m_temp_query.session.bind)
    return(jsonify(p12m_temp.to_dict()))

@app.route("/api/v1.0/<start>")
def temps(start):

    start_dateobject=datetime.datetime.strptime(start, '%Y-%m-%d')
    start_newdate_object=datetime.datetime.date(start_dateobject-datetime.timedelta(days=365))

    temp_query = session.query(Measurements).filter(Measurements.date>=start_newdate_object).with_entities(Measurements.date, Measurements.tobs)
    df = pd.read_sql(temp_query.statement, temp_query.session.bind)
    TMIN=df['tobs'].min()
    TMAX=df['tobs'].max()
    TAVG=df['tobs'].mean()
    results=pd.DataFrame([TMIN,TMAX,TAVG])
    return jsonify(results.to_dict())

@app.route("/api/v1.0/<start>/<end>")
def temps2(start,end):
    start_dateobject=datetime.datetime.strptime(start, '%Y-%m-%d')
    start_newdate_object=datetime.datetime.date(start_dateobject-datetime.timedelta(days=365))
    
    end_dateobject=datetime.datetime.strptime(end, '%Y-%m-%d')
    end_newdate_object=datetime.datetime.date(end_dateobject-datetime.timedelta(days=365))

    temp_query = session.query(Measurements).filter(Measurements.date>=start_newdate_object,Measurements.date<=end_newdate_object).with_entities(Measurements.date, Measurements.tobs)
    df = pd.read_sql(temp_query.statement, temp_query.session.bind)
    TMIN=df['tobs'].min()
    TMAX=df['tobs'].max()
    TAVG=df['tobs'].mean()
    results=pd.DataFrame([TMIN,TMAX,TAVG])
    return jsonify(results.to_dict())

#@TODO: Test all this

if __name__ == "__main__":
    app.run(debug=True)
    raise NotImplementedError()




#see what's poppin by going to localhost:5000
#when you run the app, it will tell you:
#Running on http://127.0.0.1:5000/

#that's how you know the port is 5000 (replace 127.0.0.1 with localhost)