#-------------------------------------------------------------------------------
# Name:        ndbview.py
# Purpose:     Flask app providing end points for a simple NIVADATABASE UI.
#
# Author:      James Sample
#
# Created:     15/02/2018
# Copyright:   (c) James Sample and NIVA, 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------
""" This code is adpated from the Flask introductory tutorial here:

        http://flask.pocoo.org/docs/0.12/tutorial/

    The main aim is to provide "end points" for a new NIVADATABASE frontend.
"""
import os
import pandas as pd
from ndbview import ndb_queries
import cx_Oracle
from sqlalchemy import create_engine
from flask import Flask, request, session, g, redirect, jsonify
from flask import url_for, abort, render_template, flash

###################
# App configuration
###################

app = Flask(__name__) 

# Set config
# The password below has read-only access to a limited number of db tables
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
    # Deal with encodings
    os.environ['NLS_LANG'] = ".AL32UTF8"

    # Connect
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
    
        WARNING: There are around 30k stations, so this is slow.

    Returns:
        JSON.    
    """
    # Get db engine for this session
    engine = get_engine()

    # Get stations
    stn_df = ndb_queries.get_all_stations(engine)
    stn_df = stn_df[['station_id', 'station_code', 'station_name',
                     'longitude', 'latitude']]
    
    # Convert np.nan to None for valid JSON
    stn_df = stn_df.where((pd.notnull(stn_df)), None)
    
    # Reformat
    data = stn_df.to_dict(orient='list')

    return jsonify(data)

@app.route('/get_all_projects')
def get_all_projects():
    """ Gets ALL projects from the NIVADATABASE.

    Returns:
        JSON.       
    """
    # Get db engine for this session
    engine = get_engine()

    # Get projects
    proj_df = ndb_queries.get_all_projects(engine)
    proj_df = proj_df[['project_id', 'project_name']]

    # Convert np.nan to None for valid JSON
    proj_df = proj_df.where((pd.notnull(proj_df)), None)
    
    # Reformat
    data = proj_df.to_dict(orient='list')

    return jsonify(data)

@app.route('/get_project_stations', methods=['POST',])
def get_project_stations():
    """ Gets stations for the selected projects. Assumes data is POSTed
        as a JSON array of integers named 'project_id'. Optionally, can
        also pass a 'drop_dups' parameter (set to 'false' by default). 
        See docstring in ndb_queries.py for details:

            {"project_id":[87, 88, 89],
             "drop_dups": false}

    Returns:
        Stations table in JSON format

    NOTE: Can test using 'Postman'
    """
    # Get db engine for this session
    engine = get_engine()
    
    # Parse posted data
    sel_proj_json = request.get_json()
    sel_proj_df = pd.DataFrame({'project_id':sel_proj_json['project_id']})
    try:
        drop_dups = sel_proj_json['drop_dups']
    except KeyError:
        drop_dups = False

    # Get stations
    stn_df = ndb_queries.get_project_stations(sel_proj_df, engine, 
                                              drop_dups=drop_dups)
    stn_df = stn_df[['station_id', 'station_code', 'station_name',
                     'longitude', 'latitude']]

    # Convert np.nan to None for valid JSON
    stn_df = stn_df.where((pd.notnull(stn_df)), None)
    
    # Reformat
    data = stn_df.to_dict(orient='list')

    return jsonify(data)

@app.route('/get_station_projects', methods=['POST',])
def get_station_projects():
    """ Updates the projects list based on the projects originally selected
        and the current station list. Assumes data is POSTed as JSON arrays
        of integers for the currently selected 'project_id's and 
        'station_id's. 

            {"project_id":[87, 88, 89],
             "station_id":[9456, 9457, 9458]}

    Returns:
        Set intersection of 'project_id' array and projects associated with
        'station_id' array

    NOTE: Can test using 'Postman'
    """
    # Get db engine for this session
    engine = get_engine()
    
    # Parse posted data
    sel_json = request.get_json()
    sel_proj_df = pd.DataFrame({'project_id':sel_json['project_id']})
    sel_stn_df = pd.DataFrame({'station_id':sel_json['station_id']})

    # Get stations
    proj_df = ndb_queries.get_station_projects(sel_stn_df, sel_proj_df, engine)
    proj_df = proj_df[['project_id', 'project_name']]

    # Convert np.nan to None for valid JSON
    proj_df = proj_df.where((pd.notnull(proj_df)), None)
    
    # Reformat
    data = proj_df.to_dict(orient='list')

    return jsonify(data)

@app.route('/get_station_parameters', methods=['POST',])
def get_station_parameters():
    """ Gets water chemistry parameters for the selected stations. Assumes
        data is POSTed as JSON in the following format:

            {"st_dt":     "1990-01-01",
             "end_dt":    "2010-12-31",
             "station_id":[3561, 3562, 3563]}

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
    sel_stn_df = pd.DataFrame({'station_id':sel_stn_json['station_id']})

    # Get parameters
    par_df = ndb_queries.get_station_parameters2(sel_stn_df, st_dt, end_dt, engine)
    par_df = par_df[['parameter_id', 'parameter_name', 'unit']]
    
    # Convert np.nan to None for valid JSON
    par_df = par_df.where((pd.notnull(par_df)), None)

    # Reformat
    data = par_df.to_dict(orient='list')

    return jsonify(data)

@app.route('/get_chemistry_values', methods=['POST',])
def get_chemistry_values():
    """ Gets water chemistry values for the selected station-parameter-
        date combinations. Assumes data is POSTed as nested JSON in the
        following format:

            {"st_dt":       "1990-01-01",
             "end_dt":      "2010-12-31",
             "lods":        true,
             "drop_dups":   false,
             "station_id":  [3561, 3562, 3563],
             "parameter_id":[7, 8, 12, 244]}  

        If 'lods' is omitted, it is assumed to be 'true'; if 'drop_dups'
        is omitted, it is assumed to be 'false'.
        
    Returns:
        Water chemistry table in JSON format

    NOTE: Can test using the 'Postman'
    """
    # Get db engine for this session
    engine = get_engine()
    
    # Parse posted data
    sel_json = request.get_json()
    stn_df = pd.DataFrame({'station_id':sel_json['station_id']})
    par_df = pd.DataFrame({'parameter_id':sel_json['parameter_id']})
    st_dt = sel_json['st_dt']
    end_dt = sel_json['end_dt']
    try:
        drop_dups = sel_json['drop_dups']
    except KeyError:
        drop_dups = False
    try:
        lod_flags = sel_json['lods']
    except KeyError:
        lod_flags = True

    # Get chemistry values
    wc_df, dup_df = ndb_queries.get_chemistry_values2(stn_df, par_df,
                                                      st_dt, end_dt,
                                                      lod_flags, engine,
                                                      drop_dups=drop_dups)
    
    # Convert np.nan to None for valid JSON
    wc_df = wc_df.where((pd.notnull(wc_df)), None)

    # Reformat
    data = wc_df.to_dict(orient='list')

    return jsonify(data)
