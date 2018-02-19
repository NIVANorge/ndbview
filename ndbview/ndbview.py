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

    Returns:
        SQLAlchemy engine object as part of app context.
    """
    if not hasattr(g, 'ndb_engine'):
        g.ndb_engine = connect_ndb()
    return g.ndb_engine

###################
# Routes/end points
###################

@app.route('/', methods=['GET', 'POST'])
@app.route('/login', methods=['GET', 'POST'])
def login():
    """ Toy log-in page from the Flask tutorial. Logs-in to the NIVADATABASE
        using a read-only account.

        NOT FOR PRODUCTION - DEVELOPMENT AND TESTING ONLY!
    """
    # Parse form data when submitted
    error = None
    if request.method == 'POST':
        if request.form['username'] != app.config['USERNAME']:
            error = 'Invalid username'
        elif request.form['password'] != app.config['PASSWORD']:
            error = 'Invalid password'
        else:
            session['logged_in'] = True
            flash('You were logged in.')
            return redirect(url_for('get_all_projects_stations'))

    # Otherwise, return login page with error
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    """ Toy log-out link from Flask tutorial. Closes database connection and
        redirects to the log-in page.

        NOT FOR PRODUCTION - DEVELOPMENT AND TESTING ONLY!
    """
    # Close db connection
    if hasattr(g, 'ndb_engine'):
        g.ndb_engine.close()

    # Log out
    session.pop('logged_in', None)
    flash('You were logged out.')
    
    return redirect(url_for('login'))

@app.route('/get_all_projects_stations', methods=['GET', 'POST'])
def get_all_projects_stations():
    """ Gets ALL projects and ALL stations from the NIVADATABASE.

        Currently slow, so might need a bit of optimisation (e.g. a
        materialised view?).

    Returns:
        Nested JSON: {'stations':station_data, 'projects':project_data}
        
        For testing, can uncomment the alternative 'return' statement
        in the code below to render two massive HTML tables instead of
        raw JSON.        
    """
    # Get db engine for this session
    engine = get_engine()

    # Get projects
    proj_df = ndb_queries.get_all_projects(engine)
    proj_df = proj_df[['project_id', 'o_number', 'project_name']]

    # Get stations
    stn_df = ndb_queries.get_all_stations(engine)
    stn_df = stn_df[['station_id', 'station_code', 'station_name',
                     'latitude', 'longitude']]

    # Combine
    data = {'stations':stn_df.to_dict(orient='records'),
            'projects':proj_df.to_dict(orient='records')}
    
    return render_template('show_projects_stations.html',
                           tables=[proj_df.to_html(), stn_df.to_html()],
                           titles=['Projects', 'Stations'])

#    return jsonify(data)

@app.route('/get_project_stations', methods=['POST',])
def get_project_stations():
    """ Gets stations for the selected projects. Assumes data is POSTed
        as a JSON list of project IDs e.g:

            [{"project_id":87},
             {"project_id":88},
             {"project_id":89}]

    Returns:
        Stations table in JSON format

    NOTE: Can test using the 'Postman' Chrome app (see chrome://apps/)
    """
    # Get db engine for this session
    engine = get_engine()
    
    # Parse posted data
    sel_proj_json = request.get_json()
    sel_proj_df = pd.DataFrame(sel_proj_json)

    # Get stations
    stn_df = ndb_queries.get_project_stations(sel_proj_df, engine)
    stn_df = stn_df[['station_id', 'station_code', 'station_name',
                     'latitude', 'longitude']]

    # Reformat
    data = stn_df.to_dict(orient='records')

    return jsonify(data)

@app.route('/get_station_parameters', methods=['POST',])
def get_station_parameters():
    """ Gets water chemistry parameters for the selected stations. Assumes
        data is POSTed as a JSON list of station IDs e.g:

            [{"station_id":3561},
             {"station_id":3562},
             {"station_id":3563}]

    Returns:
        Parameters table in JSON format

    NOTE: Can test using the 'Postman' Chrome app (see chrome://apps/)
    """
    # Get db engine for this session
    engine = get_engine()
    
    # Parse posted data
    sel_stn_json = request.get_json()
    sel_stn_df = pd.DataFrame(sel_stn_json)

    # Get parameters
    par_df = ndb_queries.get_station_parameters(sel_stn_df, engine)
    par_df = par_df[['parameter_id', 'parameter_name', 'unit']]

    # Reformat
    data = par_df.to_dict(orient='records')

    return jsonify(data)

@app.route('/get_chemistry_values', methods=['POST',])
def get_chemistry_values():
    """ Gets water chemistry values for the selected station-parameter-
        date combinations. Assumes data is POSTed as nested JSON in the
        following format:

            [{"start_date":"1990-01-01",
              "end_date":  "2010-12-31",
              "lod_flags":true,
              "stations":  [{"station_id":3561},
                            {"station_id":3562},
                            {"station_id":3563}],
              "parameters":[{"parameter_id":7},
                            {"parameter_id":8},
                            {"parameter_id":12},
                            {"parameter_id":244}]}]    
             
    Returns:
        Water chemistry table in JSON format

    NOTE: Can test using the 'Postman' Chrome app (see chrome://apps/)
    """
    # Get db engine for this session
    engine = get_engine()
    
    # Parse posted data
    sel_json = request.get_json()[0]
    stn_df = pd.DataFrame(sel_json['stations'])
    par_df = pd.DataFrame(sel_json['parameters'])
    st_dt = sel_json['start_date']
    end_dt = sel_json['end_date']
    lod_flags = sel_json['lod_flags']

    # Get chemistry values
    wc_df, dup_df = ndb_queries.get_chemistry_values(stn_df, par_df,
                                                     st_dt, end_dt,
                                                     lod_flags, engine)

    # Reformat
    data = wc_df.to_dict(orient='records')

    return jsonify(data)
