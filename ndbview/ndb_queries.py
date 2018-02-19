#-------------------------------------------------------------------------------
# Name:        ndb_queries.py
# Purpose:     Functions to query the NIVADATABASE.
#
# Author:      James Sample
#
# Created:     16/02/2018
# Copyright:   (c) James Sample and NIVA, 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------
""" The main aim initially is to duplicate key functionality from RESA2. This
    can then be extended.
"""
import pandas as pd
import datetime as dt
    
def get_all_projects(engine):
    """ Get full list of projects from the NDB.
    
    Args:
        engine: Obj. Active NDB "engine" object
        
    Returns:
        Dataframe
    """   
    # Query db
    sql = ("SELECT a.project_id, "
           "  b.o_number, "
           "  a.project_name, "
           "  a.project_description "
           "FROM nivadatabase.projects a, "
           "  nivadatabase.projects_o_numbers b "
           "WHERE a.project_id = b.project_id "
           "ORDER BY a.project_id")
    df = pd.read_sql(sql, engine)

    # Decode special characters
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.decode('Latin-1')

    return df

def get_all_stations(engine):
    """ Get full list of stations from the NDB.
    
    Args:
        engine: Obj. Active NDB "engine" object
        
    Returns:
        Dataframe
    """   
    # Query db
    sql = ("SELECT DISTINCT a.station_id, "
           "  a.station_code, "
           "  a.station_name, "
           "  c.station_type, "
           "  d.latitude, "
           "  d.longitude "
           "FROM nivadatabase.projects_stations a, "
           "  nivadatabase.stations b, "
           "  nivadatabase.station_types c, "
           "  niva_geometry.sample_points d "
           "WHERE a.station_id = b.station_id "
           "AND b.station_type_id = c.station_type_id "
           "AND b.geom_ref_id     = d.sample_point_id "
           "ORDER BY a.station_id")
    df = pd.read_sql(sql, engine)

    # Decode special characters
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.decode('Latin-1')

    return df

def get_project_stations(proj_df, engine):
    """ Get stations asscoiated with selected projects.
    
    Args:
        proj_df: Dataframe. Must have a column named 'project_id' with
                 the project IDs of interest
        engine:  Obj. Active NDB "engine" object
        
    Returns:
        Dataframe
    """       
    # Get proj IDs
    assert len(proj_df) > 0, 'ERROR: Please select at least one project.'
    proj_df['project_id'].drop_duplicates(inplace=True)
    proj_ids = proj_df['project_id'].values.astype(int)

    # Build query
    bind_pars = ','.join(':%d' % i for i in xrange(len(proj_ids)))
    
    # Query db
    sql = ("SELECT DISTINCT a.station_id, "
           "  a.station_code, "
           "  a.station_name, "
           "  c.station_type, "
           "  d.longitude, "
           "  d.latitude "
           "FROM nivadatabase.projects_stations a, "
           "  nivadatabase.stations b, "
           "  nivadatabase.station_types c, "
           "  niva_geometry.sample_points d "
           "WHERE a.station_id IN "
           "  (SELECT station_id "
           "  FROM nivadatabase.projects_stations "
           "  WHERE project_id IN (%s) "
           "  ) " 
           "AND a.station_id      = b.station_id "
           "AND b.station_type_id = c.station_type_id "
           "AND b.geom_ref_id     = d.sample_point_id "
           "ORDER BY a.station_id" % bind_pars)
    df = pd.read_sql(sql, params=proj_ids, con=engine)

    # Decode special characters
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.decode('Latin-1')
                   
    return df

def get_station_parameters(stn_df, engine):
    """ Gets the list of available water chemistry parameters for the
        selected stations.

    Args:
        stn_df: Dataframe. Must have a column named 'station_id' with
                the station IDs of interest
        engine: Obj. Active NDB "engine" object
        
    Returns:
        Dataframe
    """       
    # Get stn IDs
    assert len(stn_df) > 0, 'ERROR: Please select at least one station.'
    stn_df['station_id'].drop_duplicates(inplace=True)
    stn_ids = stn_df['station_id'].values.astype(int)

    # Build query
    bind_pars = ','.join(':%d' % i for i in xrange(len(stn_ids)))

    # Query db
    sql = ("SELECT parameter_id, "
           "  name AS parameter_name, "
           "  unit "
           "FROM NIVADATABASE.WC_PARAMETER_DEFINITIONS "
           "WHERE PARAMETER_ID IN "
           "  ( SELECT DISTINCT parameter_id "
           "  FROM NIVADATABASE.WC_PARAMETERS_METHODS "
           "  WHERE METHOD_ID IN "
           "    ( SELECT DISTINCT b.method_id "
           "    FROM nivadatabase.water_samples a, "
           "      NIVADATABASE.WATER_CHEMISTRY_VALUES b "
           "    WHERE a.station_id   IN (%s) "
           "    AND a.WATER_SAMPLE_ID = b.WATER_SAMPLE_ID "
           "    ) "
           "  ) "
           "ORDER BY name" % bind_pars)
    df = pd.read_sql(sql, params=stn_ids, con=engine)

    # Decode special characters
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.decode('Latin-1')
                   
    return df

def get_chemistry_values(stn_df, par_df, st_dt,
                         end_dt, lod_flags, engine):
    """ Get water chemistry data for selected station-parameter-
        date combinations.

    Args:
        stn_df:    Dataframe. Must have a column named 'station_id' with
                   the station IDs of interest 
        par_df:    Dataframe. Must have a column named 'parameter_id' with
                   the parameter IDs of interest
        st_dt:     Str. Format 'YYYY-MM-DD'
        end_dt:    Str. Format 'YYYY-MM-DD'
        lod_flags: Bool. Whether to include LOD flags in output
        engine:    Obj. Active NDB "engine" object
        
    Returns:
        Dataframe
    """   
    # Get stn IDs
    assert len(stn_df) > 0, 'ERROR: Please select at least one station.'
    stn_df['station_id'].drop_duplicates(inplace=True)
    stn_ids = stn_df['station_id'].values.astype(int)

    # Get par IDs
    assert len(par_df) > 0, 'ERROR: Please select at least one parameter.'
    par_df['parameter_id'].drop_duplicates(inplace=True)
    par_ids = par_df['parameter_id'].values.astype(int)

    # Convert dates
    st_dt = dt.datetime.strptime(st_dt, '%Y-%m-%d')
    end_dt = dt.datetime.strptime(end_dt, '%Y-%m-%d')

    # Number from 0 to n_stns
    bind_stns = ','.join(':%d' % i for i in xrange(len(stn_ids)))
    
    # Number from n_stns to (n_stns+n_params)
    bind_pars = ','.join(':%d' % i for i in
                         xrange(len(stn_ids), len(stn_ids)+len(par_ids)))

    # Query db
    sql = ("SELECT a.station_id, "
           "  a.sample_date, "
           "  a.depth1, "
           "  a.depth2, "
           "  b.name AS parameter_name, "
           "  b.unit, "
           "  c.flag1, "
           "  (c.value*d.conversion_factor) AS value "
           "FROM nivadatabase.water_samples a, "
           "  NIVADATABASE.WC_PARAMETER_DEFINITIONS b, "
           "  NIVADATABASE.WATER_CHEMISTRY_VALUES c, "
           "  NIVADATABASE.WC_PARAMETERS_METHODS d " 
           "WHERE a.water_sample_id = c.WATER_SAMPLE_ID " 
           "AND c.method_id         = d.METHOD_ID "
           "AND d.PARAMETER_ID      = b.PARAMETER_ID " 
           "AND a.station_id       IN (%s) "
           "AND b.parameter_id     IN (%s) "
           "AND c.approved          = 1 " 
           "AND a.sample_date      >= :st_dt "
           "AND a.sample_date      <= :end_dt" % (bind_stns, bind_pars))

    # Build query params
    par_dict = {'end_dt':end_dt,
                'st_dt':st_dt}
    bind_stn_dict = {'%d' % idx:item for idx, item in enumerate(stn_ids)}
    bind_par_dict = {'%d' % (idx + len(stn_ids)):item 
                     for idx, item in enumerate(par_ids)}
    par_dict.update(bind_stn_dict)
    par_dict.update(bind_par_dict)    
    df = pd.read_sql(sql, params=par_dict, con=engine)

    # Decode special characters
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.decode('Latin-1')

    # Drop exact duplicates (i.e. including value)
    df.drop_duplicates(inplace=True)

    # Check for conflicting duplicates (i.e. same location, different value)
    dup_df = df[df.duplicated(subset=['station_id',
                                      'sample_date',
                                      'depth1',
                                      'depth2',
                                      'parameter_name',
                                      'unit'], 
                              keep=False)].sort_values(by=['station_id',
                                                           'sample_date',
                                                           'depth1',
                                                           'depth2',
                                                           'parameter_name',
                                                           'unit']) 

    if len(dup_df) > 0:
        print ('    WARNING\n    The database contains duplicated values for some station-'
               'date-parameter combinations.\n    Only the most recent values '
               'will be used, but you should check the repeated values are not '
               'errors.\n    The duplicated entries are returned in a separate '
               'dataframe.\n')
        
        # Drop duplicates
        df.drop_duplicates(subset=['station_id',
                                   'sample_date',
                                   'depth1',
                                   'depth2',
                                   'parameter_name',
                                   'unit'],
                           keep='last', inplace=True)

    # Restructure data
    df['par_unit'] = df['parameter_name'] + '_' + df['unit']
    del df['parameter_name'], df['unit']

    # Include LOD flags?
    if lod_flags:
        df['flag1'] = df['flag1'].fillna('')
        df['value'] = df['flag1'].astype(str) + df['value'].astype(str)
        del df['flag1']
        
    else: # Ignore flags
        del df['flag1']

    # Unstack
    df.set_index(['station_id', 'sample_date', 'depth1',
                  'depth2', 'par_unit'], inplace=True)
    df = df.unstack(level='par_unit')

    # Tidy
    df.reset_index(inplace=True)
    df.index.name = ''
    df.columns = (list(df.columns.get_level_values(0)[:4]) + 
                  list(df.columns.get_level_values(1)[4:]))

    print df.head()
    
    return (df, dup_df)
