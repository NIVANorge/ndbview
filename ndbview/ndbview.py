#-------------------------------------------------------------------------------
# Name:        ndbview.py
# Purpose:     Flask app providing end points for simple NIVADATABASE UI.
#
# Author:      James Sample
#
# Created:     15/02/2018
# Copyright:   (c) James Sample and NIVA, 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------
""" This code is adpated from the Flask introductory tutorial here:

        http://flask.pocoo.org/docs/0.12/tutorial/

    The main aim is to provde "end points" for a new NIVADATABASE frontend, but
    I've also used it as an opportunity to play with Flask for the first time.
    Some tidying is probably required!
"""
import os
import pandas as pd
import ndb_queries
import cx_Oracle
from sqlalchemy import create_engine
from flask import Flask, request, session, g, redirect, jsonify
from flask import url_for, abort, render_template, flash

###################
# App configuration
###################

app = Flask(__name__) 

# Set config
app.config.update(dict(
    DATABASE=r'oracle+cx_oracle://%s:%s@nivabase:1521/nivabase',
    SECRET_KEY='development key',
    USERNAME='ndbview_user',
    PASSWORD='r0_pw',
    JSON_AS_ASCII=False))

#############################
# Manage database connections
#############################

def connect_ndb():
    """ Connects to the NIVADATABASE using a read-only user account.
    
        FOR DEVELOPMENT AND TESTING ONLY.

    Returns:
        SQLAlchemy engine object.
    """
    conn_str = app.config['DATABASE'] % (app.config['USERNAME'],
                                         app.config['PASSWORD']) 
    engine = create_engine(conn_str)
    
    return engine

def get_engine():
    """ Opens a new database connection if one does not yet exist and adds it
        to the current application context.
        
        FOR DEVELOPMENT AND TESTING ONLY.

    Returns:
        SQLAlchemy engine object as part of app context.
    """
    if not hasattr(g, 'ndb_engine'):
        g.ndb_engine = connect_ndb()
    return g.ndb_engine

###################
# Routes/end points
###################

@app.route('/get_all_stations')
def get_all_stations():
    """ Gets ALL stations from the NIVADATABASE.

    Returns:
        JSON.
        
        For testing, can uncomment the alternative 'return' statement
        in the code below to render an HTML table instead of raw JSON.       
    """
    # Get db engine for this session
    engine = get_engine()

    # Get stations
    stn_df = ndb_queries.get_all_stations(engine)
    stn_df = stn_df[['station_id', 'station_code', 'station_name',
                     'longitude', 'latitude']]
    stn_df.columns = ['id', 'c', 'n', 'x', 'y']

    # Reformat
    data = stn_df.to_dict(orient='records')
    
#    return render_template('show_projects_stations.html',
#                           tables=[stn_df.to_html(), ],
#                           titles=['Stations', ])

    return jsonify(data)

@app.route('/get_all_projects')
def get_all_projects():
    """ Gets ALL projects from the NIVADATABASE.

    Returns:
        JSON.
        
        For testing, can uncomment the alternative 'return' statement
        in the code below to render an HTML table instead of raw JSON.        
    """
    # Get db engine for this session
    engine = get_engine()

    # Get projects
    proj_df = ndb_queries.get_all_projects(engine)
    proj_df = proj_df[['project_id', 'o_number', 'project_name']]
    proj_df.columns = ['id', 'o', 'n']

    # Reformat
    data = proj_df.to_dict(orient='records')
    
#    return render_template('show_projects_stations.html',
#                           tables=[proj_df.to_html(), ],
#                           titles=['Projects', ])

    return jsonify(data)

@app.route('/get_project_stations', methods=['POST',])
def get_project_stations():
    """ Gets stations for the selected projects. Assumes data is POSTed
        as a JSON array of integers names 'id':

            {"id":[87, 88, 89]}

    Returns:
        Stations table in JSON format

    NOTE: Can test using 'Postman'
    """
    # Get db engine for this session
    engine = get_engine()
    
    # Parse posted data
    sel_proj_json = request.get_json()
    sel_proj_df = pd.DataFrame(sel_proj_json)

    # Get stations
    stn_df = ndb_queries.get_project_stations(sel_proj_df, engine)
    stn_df = stn_df[['station_id', 'station_code', 'station_name',
                     'longitude', 'latitude']]
    stn_df.columns = ['id', 'c', 'n', 'x', 'y']

    # Reformat
    data = stn_df.to_dict(orient='records')

    return jsonify(data)

@app.route('/get_station_parameters', methods=['POST',])
def get_station_parameters():
    """ Gets water chemistry parameters for the selected stations. Assumes
        data is POSTed as a JSON in the following format:

            {"st_dt":   "1990-01-01",
             "end_dt":  "2010-12-31",
             "id":[3561, 3562, 3563]}

    Returns:
        Parameters table in JSON format

    NOTE: Can test using the 'Postman'
    """
    # Get db engine for this session
    engine = get_engine()
    
    # Parse posted data
    sel_stn_json = request.get_json()
    st_dt = sel_stn_json['st_dt']
    end_dt = sel_stn_json['end_dt']
    sel_stn_df = pd.DataFrame({'id':sel_stn_json['id']})

    # Get parameters
    par_df = ndb_queries.get_station_parameters2(sel_stn_df, st_dt, end_dt, engine)
    par_df = par_df[['parameter_id', 'parameter_name', 'unit']]
    par_df.columns = ['id', 'n', 'u']

    # Reformat
    data = par_df.to_dict(orient='records')

    return jsonify(data)

@app.route('/get_chemistry_values', methods=['POST',])
def get_chemistry_values():
    """ Gets water chemistry values for the selected station-parameter-
        date combinations. Assumes data is POSTed as nested JSON in the
        following format:

            {"st_dt": "1990-01-01",
             "end_dt":"2010-12-31",
             "lods":  true,
             "stns":  [3561, 3562, 3563],
             "pars":  [7, 8, 12, 244]}  
             
    Returns:
        Water chemistry table in JSON format

    NOTE: Can test using the 'Postman'
    """
    # Get db engine for this session
    engine = get_engine()
    
    # Parse posted data
    sel_json = request.get_json()
    stn_df = pd.DataFrame({'stns':sel_json['stns']})
    par_df = pd.DataFrame({'pars':sel_json['pars']})
    st_dt = sel_json['st_dt']
    end_dt = sel_json['end_dt']
    lod_flags = sel_json['lods']

    # Get chemistry values
    wc_df, dup_df = ndb_queries.get_chemistry_values2(stn_df, par_df,
                                                      st_dt, end_dt,
                                                      lod_flags, engine)

    # Reformat
    data = wc_df.to_dict(orient='records')

    return jsonify(data)
