#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2021 Maxime Marin
#
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

def download_NOAA_OISST(lon_window,lat_window,start=None,end=None,filename = "NOAA_OISST.nc"):
    import urllib
    import xarray as xr
    import io
    import datetime
    import numpy as np
    import os
    import os.path
    
    default_start = datetime.datetime(1981,9,1,12,0,0)# default start of NOAA OISST
    now = datetime.datetime.now()
    if start is None:
        start = default_start
    if end is None:
        end = now
    
    #First check if there is already a file named like this
    if os.path.isfile("./" + filename): #yes, let's open the data then
        print('updating the file...')
        data_old = xr.open_dataset(filename)
        start = data_old.time[-1]+datetime.timedelta(days = 1)
              
    if np.diff(lon_window)<0 or np.diff(lat_window)<0 or start-end>datetime.timedelta(0):
        raise ValueError('order of coordinates window wrong')
        
    lat_string = "({:.1f}):1:({:.1f})".format(lat_window[0],lat_window[1])
    lon_string = "({:.1f}):1:({:.1f})".format(lon_window[0],lon_window[1])
    
    lat_query = '%5B' + lat_string + '%5D'
    lon_query = '%5B' + lon_string + '%5D'
    z_query = '%5B(0.0):1:(0.0)%5D'

    for y in range(start.year,end.year+1):#if the query is too long in the time dimension, there will be a HTTP error. To avoid it, we reduce the size (time period) of our query
        year_st = datetime.datetime(y,1,1,12,0,0) if y>start.year else start
        year_end = datetime.datetime(y,12,31,12,0,0) if y<now.year else now-datetime.timedelta(days=60) # the product is not near real-time but slightly delayed. We make sure we don't raise errors because of that.
            
        end_string = year_end.strftime("(%Y-%m-%dT%H:%M:%SZ)")
        start_string = year_st.strftime("(%Y-%m-%dT%H:%M:%SZ)")

        time_query = '%5B' + start_string + ':1:' + end_string + '%5D'
        url = "https://coastwatch.pfeg.noaa.gov/erddap/griddap/ncdcOisst21Agg_LonPM180.nc"
        path = url + '?' + 'sst' + time_query + z_query + lat_query + lon_query
        
        req = urllib.request.Request(path)
        try:
            x = urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            print(e.code)
            print(e.read())  
            
        ds = xr.open_dataset(io.BytesIO(x.read())).to_array()
        print(y)
        
        if y>start.year:
            da = xr.concat([da,ds],dim = 'time')
        else:
            da = ds
              
    if 'data_old' in locals():# update
          da  = xr.concat([data_old,da],dim = 'time')

    # let's save the data
    frmt = filename.partition('.')[-1]
    if frmt == 'nc':
        da.to_netcdf(filename)
    elif frmt == 'csv':
        da = da.to_dataframe()
        da.to_csv(filename,float_format = '%.2f', na_rep = 'NaN')
              
    return da