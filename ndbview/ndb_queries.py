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
#    sql = ("SELECT a.project_id, "
#           "  b.o_number, "
#           "  a.project_name, "
#           "  a.project_description "
#           "FROM nivadatabase.projects a, "
#           "  nivadatabase.projects_o_numbers b "
#           "WHERE a.project_id = b.project_id "
#           "ORDER BY a.project_id")
    sql = ("SELECT project_id, "
           "  project_name, "
           "  project_description "
           "FROM nivadatabase.projects "
           "ORDER BY project_id")
    df = pd.read_sql(sql, engine)

    return df

def get_all_stations(engine):
    """ Get full list of stations from the NDB.
    
        Note: The NIVADATABASE allows multiple names for the same station.
        This function returns all unique id-code-name-type-lat-lon
        combinations, which will include duplicated station IDs in some 
        cases.
    
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
           "WHERE a.station_id    = b.station_id "
           "AND b.station_type_id = c.station_type_id "
           "AND b.geom_ref_id     = d.sample_point_id "
           "ORDER BY a.station_id")
    df = pd.read_sql(sql, engine)

    return df

#def get_station_props(stn_ids, engine):
#    """ Get properties for specified stations from the NDB.
#    
#    Args:
#        stn_ids: Array-like. 1D integer array of station IDs
#        engine:  Obj. Active NDB "engine" object
#        
#    Returns:
#        Dataframe
#    """ 
#    # Build query
#    bind_pars = ','.join(':%d' % i for i in range(len(stn_ids)))
#    
#    # Query db
#    sql = ("SELECT DISTINCT a.station_id, "
#           "  a.station_code, "
#           "  a.station_name, "
#           "  c.station_type, "
#           "  d.latitude, "
#           "  d.longitude "
#           "FROM nivadatabase.projects_stations a, "
#           "  nivadatabase.stations b, "
#           "  nivadatabase.station_types c, "
#           "  niva_geometry.sample_points d "
#           "WHERE a.station_id    = b.station_id "
#           "AND b.station_type_id = c.station_type_id "
#           "AND b.geom_ref_id     = d.sample_point_id "
#           "AND a.station_id     IN (%s)" % bind_pars)
#    df = pd.read_sql(sql, params=stn_ids, con=engine)
#    
#    # Station IDs are unique locations, but they can have different names
#    # in different projects. This code drops duplicated IDs, but chooses
#    # station name and code at random from among the duplicates
#    df.drop_duplicates(subset='station_id', inplace=True)
#
#    # Decode special characters
#    for col in df.columns:
#        if df[col].dtype == object:
#            df[col] = df[col].str.decode('Latin-1')
#
#    return df   
    
def get_project_stations(proj_df, engine, drop_dups=False):
    """ Get stations asscoiated with selected projects.
    
    Args:
        proj_df:   Dataframe. Must have a column named 'project_id' with
                   the project IDs of interest
        engine:    Obj. Active NDB "engine" object
        drop_dups: Bool. The same station may have different names in different
                   projects. If some of the selected projects include the 
                   same station, this will result in duplicates in the 
                   stations table (i.e. same station ID, but multiple names).
                   By default, the duplicates will be returned. Setting
                   'drop_dups=True' will select one set of names per station
                   ID and return a dataframe with no duplicates (but the 
                   station codes and names may not be what you're expecting)
        
    Returns:
        Dataframe
    """       
    # Get proj IDs
    assert len(proj_df) > 0, 'ERROR: Please select at least one project.'
    proj_df['project_id'].drop_duplicates(inplace=True)
    proj_ids = proj_df['project_id'].values.astype(int).tolist()

    # Query db
    bind_pars = ','.join(':%d' % i for i in range(len(proj_ids)))    

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

    # Drop duplictaes, if desired
    if drop_dups:
        df.drop_duplicates(subset='station_id', inplace=True)
                       
    return df

def get_station_projects(stn_df, proj_df, engine):
    """ Get projects asscoiated with selected stations.
    
    Args:
        stn_df:  Dataframe. Must have a column named 'station_id' with
                 the station IDs of interest
        proj_df: Dataframe. Must have a column named 'project_id' with
                 the currently selected project IDs of interest
        engine:  Obj. Active NDB "engine" object
        
    Returns:
        Dataframe
    """       
    # Get stn IDs
    assert len(stn_df) > 0, 'ERROR: Please select at least one station.'
    stn_df['station_id'].drop_duplicates(inplace=True)
    stn_ids = stn_df['station_id'].values.astype(int).tolist()

    # Get proj IDs
    assert len(proj_df) > 0, 'ERROR: At least one project must already be selected.'
    proj_df['project_id'].drop_duplicates(inplace=True)
    proj_ids = proj_df['project_id'].values.astype(int).tolist()  

    # Number from 0 to n_stns
    bind_stns = ','.join(':%d' % i for i in range(len(stn_ids)))
    
    # Number from n_stns to (n_stns+n_projs)
    bind_prjs = ','.join(':%d' % i for i in range(len(stn_ids), 
                                                   len(stn_ids) + len(proj_ids)))
    
    # Query db
    sql = ("SELECT a.project_id, "
           "  b.o_number, "
           "  a.project_name, "
           "  a.project_description "
           "FROM nivadatabase.projects a, "
           "  nivadatabase.projects_o_numbers b "
           "WHERE a.project_id = b.project_id "
           "AND a.project_id  IN "
           "  (SELECT project_id "
           "  FROM nivadatabase.projects_stations "
           "  WHERE station_id IN (%s) "
           "  AND project_id   IN (%s) "
           "  ) "
           "ORDER BY a.project_id" % (bind_stns, bind_prjs))

    bind_dict = {'%d' % idx:item for idx, item in enumerate(stn_ids)}
    bind_prj_dict = {'%d' % (idx + len(stn_ids)):item 
                     for idx, item in enumerate(proj_ids)}
    bind_dict.update(bind_prj_dict) 
    df = pd.read_sql(sql, params=bind_dict, con=engine)
                       
    return df

#def get_station_parameters(stn_df, st_dt, end_dt, engine):
#    """ Gets the list of available water chemistry parameters for the
#        selected stations.
#
#    Args:
#        stn_df: Dataframe. Must have a column named 'station_id' with
#                the station IDs of interest
#        engine: Obj. Active NDB "engine" object
#        
#    Returns:
#        Dataframe
#    """       
#    # Get stn IDs
#    assert len(stn_df) > 0, 'ERROR: Please select at least one station.'
#    stn_df['station_id'].drop_duplicates(inplace=True)
#    stn_ids = stn_df['station_id'].values.astype(int).tolist()
#
#    # Convert dates
#    st_dt = dt.datetime.strptime(st_dt, '%Y-%m-%d')
#    end_dt = dt.datetime.strptime(end_dt, '%Y-%m-%d')
#    
#    # Query db
#    bind_pars = ','.join(':%d' % i for i in range(len(stn_ids)))
#    
#    sql = ("SELECT parameter_id, "
#           "  name AS parameter_name, "
#           "  unit "
#           "FROM NIVADATABASE.WC_PARAMETER_DEFINITIONS "
#           "WHERE PARAMETER_ID IN "
#           "  ( SELECT DISTINCT parameter_id "
#           "    FROM NIVADATABASE.WC_PARAMETERS_METHODS "
#           "    WHERE METHOD_ID IN "
#           "    ( SELECT DISTINCT b.method_id "
#           "      FROM nivadatabase.water_samples a, "
#           "      NIVADATABASE.WATER_CHEMISTRY_VALUES b "
#           "      WHERE a.station_id   IN (%s) "
#           "      AND a.sample_date    >= :st_dt "
#           "      AND a.sample_date    <= :end_dt " 
#           "      AND b.approved        = 1"
#           "      AND a.WATER_SAMPLE_ID = b.WATER_SAMPLE_ID "
#           "    ) "
#           "  ) "
#           "ORDER BY name" % bind_pars)
#
#    par_dict = {'end_dt':end_dt,
#                'st_dt':st_dt}
#    bind_dict = {'%d' % idx:item for idx, item in enumerate(stn_ids)}
#    par_dict.update(bind_dict)    
#    df = pd.read_sql(sql, params=par_dict, con=engine) 
#
#    # Decode special characters
#    for col in df.columns:
#        if df[col].dtype == object:
#            df[col] = df[col].str.decode('Latin-1')
#                   
#    return df

def get_station_parameters2(stn_df, st_dt, end_dt, engine):
    """ Gets the list of available water chemistry parameters for the
        selected stations.

        NOTE: This is an alternative to get_station_parameters().
        
        It looks as though Tore/Roar have already written code to 
        summarise data into NIVADATABASE.WCV_CALK. Assuming this table 
        is reliable, it is easier to query directly than to refactor lots
        of PL/SQL into Python.

    Args:
        stn_df: Dataframe. Must have a column named 'station_id' with
                the station IDs of interest
        st_dt:  Str. Format 'YYYY-MM-DD'
        end_dt: Str. Format 'YYYY-MM-DD'
        engine: Obj. Active NDB "engine" object
        
    Returns:
        Dataframe
    """ 
    # Get stn IDs
    assert len(stn_df) > 0, 'ERROR: Please select at least one station.'
    stn_df['station_id'].drop_duplicates(inplace=True)
    stn_ids = stn_df['station_id'].values.astype(int).tolist()

    # Convert dates
    st_dt = dt.datetime.strptime(st_dt, '%Y-%m-%d')
    end_dt = dt.datetime.strptime(end_dt, '%Y-%m-%d')
    
    # Query db
    bind_pars = ','.join(':%d' % i for i in range(len(stn_ids)))    

    bind_pars = ','.join(':%d' % i for i in range(len(stn_ids)))
    sql = ("SELECT DISTINCT parameter_id, "
           "  name AS parameter_name, "
           "  unit "
           "FROM nivadatabase.wcv_calk "
           "WHERE station_id IN (%s) "
           "AND sample_date  >= :st_dt "
           "AND sample_date  <= :end_dt " 
           "ORDER BY name, "
           "  unit" % bind_pars)

    par_dict = {'end_dt':end_dt,
                'st_dt':st_dt}
    bind_dict = {'%d' % idx:item for idx, item in enumerate(stn_ids)}
    par_dict.update(bind_dict)    
    df = pd.read_sql(sql, params=par_dict, con=engine)    
            
    return df

#def get_chemistry_values(stn_df, par_df, st_dt,
#                         end_dt, lod_flags, engine):
#    """ Get water chemistry data for selected station-parameter-
#        date combinations.
#
#    Args:
#        stn_df:    Dataframe. Must have a column named 'station_id' with
#                   the station IDs of interest 
#        par_df:    Dataframe. Must have a column named 'parameter_id' with
#                   the parameter IDs of interest
#        st_dt:     Str. Format 'YYYY-MM-DD'
#        end_dt:    Str. Format 'YYYY-MM-DD'
#        lod_flags: Bool. Whether to include LOD flags in output
#        engine:    Obj. Active NDB "engine" object
#        
#    Returns:
#        Tuple of dataframes (wc_df, dup_df)
#    """   
#    # Get stn IDs
#    assert len(stn_df) > 0, 'ERROR: Please select at least one station.'
#    stn_df['station_id'].drop_duplicates(inplace=True)
#    stn_ids = stn_df['station_id'].values.astype(int).tolist()
#    
#    # Get stn properties
#    stn_props = get_station_props(stn_ids, engine)
#
#    # Get par IDs
#    assert len(par_df) > 0, 'ERROR: Please select at least one parameter.'
#    par_df['parameter_id'].drop_duplicates(inplace=True)
#    par_ids = par_df['parameter_id'].values.astype(int).tolist()
#
#    # Convert dates
#    st_dt = dt.datetime.strptime(st_dt, '%Y-%m-%d')
#    end_dt = dt.datetime.strptime(end_dt, '%Y-%m-%d')
#
#    # Number from 0 to n_stns
#    bind_stns = ','.join(':%d' % i for i in range(len(stn_ids)))
#    
#    # Number from n_stns to (n_stns+n_params)
#    bind_pars = ','.join(':%d' % i for i in
#                         range(len(stn_ids), len(stn_ids)+len(par_ids)))
#
#    # Query db
#    sql = ("SELECT a.station_id AS id, "
#           "  a.sample_date AS date, "
#           "  a.depth1, "
#           "  a.depth2, "
#           "  b.name, "
#           "  b.unit, "
#           "  c.flag1, "
#           "  (c.value*d.conversion_factor) AS value, "
#           "  c.entered_date "
#           "FROM nivadatabase.water_samples a, "
#           "  NIVADATABASE.WC_PARAMETER_DEFINITIONS b, "
#           "  NIVADATABASE.WATER_CHEMISTRY_VALUES c, "
#           "  NIVADATABASE.WC_PARAMETERS_METHODS d " 
#           "WHERE a.water_sample_id = c.WATER_SAMPLE_ID " 
#           "AND c.method_id         = d.METHOD_ID "
#           "AND d.PARAMETER_ID      = b.PARAMETER_ID " 
#           "AND a.station_id       IN (%s) "
#           "AND b.parameter_id     IN (%s) "
#           "AND c.approved          = 1 " 
#           "AND a.sample_date      >= :st_dt "
#           "AND a.sample_date      <= :end_dt" % (bind_stns, bind_pars))
#
#    par_dict = {'end_dt':end_dt,
#                'st_dt':st_dt}
#    bind_stn_dict = {'%d' % idx:item for idx, item in enumerate(stn_ids)}
#    bind_par_dict = {'%d' % (idx + len(stn_ids)):item 
#                     for idx, item in enumerate(par_ids)}
#    par_dict.update(bind_stn_dict)
#    par_dict.update(bind_par_dict)    
#    df = pd.read_sql(sql, params=par_dict, con=engine)
#
#    # Decode special characters
#    for col in df.columns:
#        if df[col].dtype == object:
#            df[col] = df[col].str.decode('Latin-1')
#
#    # Drop exact duplicates (i.e. including value)
#    df.drop_duplicates(subset=['id',
#                               'date',
#                               'depth1',
#                               'depth2',
#                               'name',
#                               'unit',
#                               'flag1',
#                               'value'], inplace=True)
#    
#    # Check for conflicting duplicates (i.e. same location, different value)
#    dup_df = df[df.duplicated(subset=['id',
#                                      'date',
#                                      'depth1',
#                                      'depth2',
#                                      'name',
#                                      'unit'], 
#                              keep=False)].sort_values(by=['id',
#                                                           'date',
#                                                           'depth1',
#                                                           'depth2',
#                                                           'name',
#                                                           'unit',
#                                                           'entered_date']) 
#
#    if len(dup_df) > 0:
#        print ('WARNING\nThe database contains duplicated values for some station-'
#               'date-parameter combinations.\nOnly the most recent values '
#               'will be used, but you should check the repeated values are not '
#               'errors.\nThe duplicated entries are returned in a separate '
#               'dataframe.\n')
#        
#        # Choose most recent record for each duplicate
#        df.sort_values(by='entered_date', inplace=True, ascending=True)
#        
#        # Drop duplicates
#        df.drop_duplicates(subset=['id',
#                                   'date',
#                                   'depth1',
#                                   'depth2',
#                                   'name',
#                                   'unit'],
#                           keep='last', inplace=True)
#
#    # Restructure data
#    del df['entered_date']
#    df['name'].fillna('', inplace=True)
#    df['unit'].fillna('', inplace=True)
#    df['par_unit'] = (df['name'].astype(str) + '_' +
#                      df['unit'].astype(str))
#    del df['name'], df['unit']
#
#    # Include LOD flags?
#    if lod_flags:
#        df['flag1'].fillna('', inplace=True)
#        df['value'] = df['flag1'].astype(str) + df['value'].astype(str)
#        del df['flag1']
#        
#    else: # Ignore flags
#        del df['flag1']
#
#    # Unstack   
#    df.set_index(['id', 'date', 'depth1', 'depth2', 
#                  'par_unit'], inplace=True)
#    df = df.unstack(level='par_unit')
#
#    # Tidy
#    df.reset_index(inplace=True)
#    df.index.name = ''
#    df.columns = (list(df.columns.get_level_values(0)[:4]) + 
#                  list(df.columns.get_level_values(1)[4:]))
#    df.sort_values(by=['id', 'date'],
#                   inplace=True)
#    
#    # Join stn properties
#    # Props to include
#    props = ['id', 'code', 'stn_name']
#    df = pd.merge(df, stn_props[props], how='left', on='id')
#    
#    # Reorder
#    cols = [col for col in df.columns if col not in props]
#    cols = props + cols
#    df = df[cols]    
#    
#    return (df, dup_df)

def get_chemistry_values2(stn_df, par_df, st_dt, end_dt, 
                          lod_flags, engine, drop_dups=False):
    """ Get water chemistry data for selected station-parameter-
        date combinations. 
        
        NOTE: This is an alternative to get_chemistry_values().
        
        It looks as though Tore/Roar have already written code to deal
        with duplicates etc., similar to what I've implemented above in
        get_chemistry_values(). See NIVADATABASE.PKG_WC_COMPUTED (which 
        updates NIVADATABASE.WCV_CALK) for details.
        
        I think there may still be some issues with WCV_CALK, but one
        advantage is that it includes *some* of the "calculated" 
        parameters available in RESA2 (e.g. ANC). 
        
        Assuming this table is reliable, it is easier to query directly
        than to refactor lots of PL/SQL into Python.
        
    Args:
        stn_df:    Dataframe. Must have a column named 'station_id' with
                   the station IDs of interest 
        par_df:    Dataframe. Must have a column named 'parameter_id' with
                   the parameter IDs of interest
        st_dt:     Str. Format 'YYYY-MM-DD'
        end_dt:    Str. Format 'YYYY-MM-DD'
        lod_flags: Bool. Whether to include LOD flags in output
        engine:    Obj. Active NDB "engine" object
        drop_dups: Bool. Whether to retain duplicated rows in cases where
                   the same station ID is present with multiple names
        
    Returns:
        Tuple of dataframes (wc_df, dup_df)   
    """
    # Get stn IDs
    assert len(stn_df) > 0, 'ERROR: Please select at least one station.'
    stn_df['station_id'].drop_duplicates(inplace=True)
    stn_ids = stn_df['station_id'].values.astype(int).tolist()
    
    # Get stn properties
    #stn_props = get_station_props(stn_ids, engine)

    # Get par IDs
    assert len(par_df) > 0, 'ERROR: Please select at least one parameter.'
    par_df['parameter_id'].drop_duplicates(inplace=True)
    par_ids = par_df['parameter_id'].values.astype(int).tolist()

    # Convert dates
    st_dt = dt.datetime.strptime(st_dt, '%Y-%m-%d')
    end_dt = dt.datetime.strptime(end_dt, '%Y-%m-%d')
       
    # Number from 0 to n_stns
    bind_stns = ','.join(':%d' % i for i in range(len(stn_ids)))
    
    # Number from n_stns to (n_stns+n_params)
    bind_pars = ','.join(':%d' % i for i in range(len(stn_ids), 
                                                  len(stn_ids) + len(par_ids)))

    # Query db
    sql = ("SELECT a.station_id, "
           "  a.station_code, "
           "  a.station_name, "
           "  b.sample_date, "
           "  b.depth1, " 
           "  b.depth2, "
           "  b.name AS parameter_name, "
           "  b.unit, "
           "  b.flag1, "
           "  b.value, "
           "  b.entered_date "
           "FROM nivadatabase.projects_stations a, "
           "  nivadatabase.wcv_calk b "
           "WHERE a.station_id  = b.station_id "
           "AND a.station_id   IN (%s) "
           "AND b.parameter_id IN (%s) "
           "AND sample_date    >= :st_dt "
           "AND sample_date    <= :end_dt" % (bind_stns, bind_pars))

    par_dict = {'end_dt':end_dt,
                'st_dt':st_dt}
    bind_stn_dict = {'%d' % idx:item for idx, item in enumerate(stn_ids)}
    bind_par_dict = {'%d' % (idx + len(stn_ids)):item 
                     for idx, item in enumerate(par_ids)}
    par_dict.update(bind_stn_dict)
    par_dict.update(bind_par_dict)    
    df = pd.read_sql(sql, params=par_dict, con=engine)

    # Drop exact duplicates (i.e. including value)
    df.drop_duplicates(subset=['station_id',
                               'station_code',
                               'station_name',
                               'sample_date',
                               'depth1',
                               'depth2',
                               'parameter_name',
                               'unit',
                               'flag1',
                               'value'], inplace=True)

    # Check for "problem" duplicates i.e. duplication NOT caused by having
    # several names for the same station
    dup_df = df[df.duplicated(subset=['station_id',
                                      'station_code',
                                      'station_name',
                                      'sample_date',
                                      'depth1',
                                      'depth2',
                                      'parameter_name',
                                      'unit'], 
                              keep=False)].sort_values(by=['station_id',
                                                           'station_code',
                                                           'station_name',
                                                           'sample_date',
                                                           'depth1',
                                                           'depth2',
                                                           'parameter_name',
                                                           'unit',
                                                           'entered_date']) 

    if len(dup_df) > 0:
        print ('WARNING\nThe database contains unexpected duplicate values for '
               'some station-date-parameter combinations.\nOnly the most recent '
               'values will be used, but you should check the repeated values are '
               'not errors.\nThe duplicated entries are returned in a separate '
               'dataframe.\n')

        # Choose most recent record for each duplicate
        df.sort_values(by='entered_date', inplace=True, ascending=True)
        
        # Drop duplicates
        df.drop_duplicates(subset=['station_id',
                                   'station_code',
                                   'station_name',
                                   'sample_date',
                                   'depth1',
                                   'depth2',
                                   'parameter_name',
                                   'unit'],
                           keep='last', inplace=True)
        
    # Drop "expected" duplicates (i.e. duplicated station names), if desired
    if drop_dups:
        df.drop_duplicates(subset=['station_id',
                                   'sample_date',
                                   'depth1',
                                   'depth2',
                                   'parameter_name',
                                   'unit'],
                           keep='last', inplace=True)        
        
    # Restructure data
    del df['entered_date']
    df['parameter_name'].fillna('', inplace=True)
    df['unit'].fillna('', inplace=True)
    df['par_unit'] = (df['parameter_name'].astype(str) + '_' +
                      df['unit'].astype(str))
    del df['parameter_name'], df['unit']

    # Include LOD flags?
    if lod_flags:
        df['flag1'].fillna('', inplace=True)
        df['value'] = df['flag1'].astype(str) + df['value'].astype(str)
        del df['flag1']
        
    else: # Ignore flags
        del df['flag1']

    # Unstack   
    df.set_index(['station_id', 
                  'station_code',
                  'station_name',
                  'sample_date',
                  'depth1',
                  'depth2', 
                  'par_unit'], 
                 inplace=True)
    df = df.unstack(level='par_unit')

    # Tidy
    df.reset_index(inplace=True)
    df.index.name = ''
    df.columns = (list(df.columns.get_level_values(0)[:6]) + 
                  list(df.columns.get_level_values(1)[6:]))
    df.sort_values(by=['station_id', 'sample_date'],
                   inplace=True)   
    
    return (df, dup_df)
