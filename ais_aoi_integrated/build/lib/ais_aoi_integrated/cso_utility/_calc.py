"""
UN-ECE: Machine Learning 2022: AIS Group
https://statswiki.unece.org/display/ML/Machine+Learning+for+Official+Statistics+Home


Helper functions for use in Stationary Marine Broadcast Method (SMBM developed by CSO)

Calculation functions
___________________
PRODUCTION

calc_list_mode - returns mode (most common value) of a list

calc_list_average - returns average (mean) of a list of numbers
calc_list_standard_deviation - returns standard deviation of a list of numbers, returns 0 if list is of length=1
calc_lower_upper_time_estimates - returns tuple of lower and upper time estimates (in hours)  from  four unix input times.

calc_stddev_boundingbox_wkt - returns polygon based on a average and standard deviation data as well known text 
calc_tri_line_wkt - retuns polyline based on three input co-ordinates as well known text
calc_point_wkt - retursn point as well known text from co-ordinates.

calc_stopped_ship_result - Does a result calcuation on trigger data once escape condition has been established.
calc_haversine_m - returns distance in metres between two lat/lng co-ordinates
calc_standard_deviation_xy - returns tuple (deltaX,deltaY) which is standard deviation of lat and lng lists in  terms of metres.

contributers
Justin McGurk: CSO (Ireland): justin.mcgurk@cso.ie

Last update: 20240108
calc_stopped_ship_result, calc_haversine_m, calc_standard_deviation_xy
Ready to be pushed back to main


"""



import numpy as np
import math
import h3
from typing import Set, Dict, List, Tuple, Optional
import logging


def calc_stopped_ship_result(h3_trigger, 
mmsi,
imo,
ais_length,
ais_width,
ais_vessel_type,
ais_vessel_type_main,
ais_obs_time,
lat_list,
lng_list,
unix_list,
navigation_status_list,
state_values,
delta_time_lower,
delta_time_upper,
min_observations,
h3_version=3,
standard_deviation_factor=1):
    
    
    """
    Function to calculate a result from stopped ship event (the_trigger) data.
    Implemnts several helper functions
    - calc_standard_deviation_xy()
        - calc_haversine_m
        - calc_list_standard_deviation
        - calc_list_average
    - calc_list_average()
    - calc_list_mode()
    - calc_list_standard_deviation()
    
    Two types of results are returned : valid and invalid.
    Valididity is assessed based on  the number of observations in stopped ship event.  If this number is less
    than the min_observations paramter used then an invalid flag is raised.
    This will impact on some statisticla calculations carried out internally in this function.
    Namley delta_x, delta_y, sd_lat, sd_lng will return as zero as standard deviations are not a sound calculation.
    wkt_triline and wkt_sd_box will return EMPTY well known text objects.
    Main purpose is to act as a health warning.
    
    
    parameters
    --------------  
    h3_trigger - h3 index of trigger event
    mmsi - mmsi value  of  stopped ship
    imo - imo value  of  stopped ship
    ais_length
    ais_width
    ais_vessel_type
    ais_vessel_type_main
    ais_obs_time
    
    lat_list - list of litidudes from  trigger object
    lng_list  - list of longitudes from  trigger object
    unix_list - list of unix times from  trigger object
    navigation_status_list - list of navigation status from trigger object
    state_values - tuple of  state values from trigger object
    
    delta_time_lower - lower time estimate of stopped ship event
    delta_time_upper - upper time estimate of stopped ship event
    min_observations - integer minimum nuber of observations in trigger that is acceptable for valid stopped ship event.
    h3_version: integer {3|4}:default=3 - version of h3 api used.  Implemneted in calc_standard_deviation_xy()
    standard_deviation_factor - Integer:default=1 - controls how many standard deviations are used in calculation of 
    wkt_sd_box



    returns
    --------------
    tuple (status, result, header_list)
    status string: 'valid'|invalid' did the stopped ship event have at least the minimum number of observations required?
    result list of the following values corresponding to header list.
        'h3_index_int',
        'mmsi',
        'imo',
        'length',
        'width',
        'vessel_type',
        'vessel_type_main',
        'obs_time', 
        'time_lower',
        'time_upper',
        'unix_trigger',
        'avg_unix',
        'unix_disarm',
        'sd_lat',
        'sd_lng',
        'delta_x',
        'delta_y',
        'trigger_lat',
        'trigger_lng',
        'avg_lat',
        'avg_lng',
        'disarm_lat',
        'disarm_lng',
        'mode_nav_status',
        'obs_nav_status',
        'obs_count',
        'state_initial',
        'state_final',
        'wkt_trigger_pt',
        'wkt_avg_pt',
        'wkt_disarm_pt',
        'wkt_triline',
        'wkt_sd_box',
        'is_valid'

    """
    result = []
    header_list =['h3_index_int',
    'mmsi',
    'imo',
    'length',
    'width',
    'vessel_type',
    'vessel_type_main',
    'obs_time', 
    'time_lower',
    'time_upper',
    'unix_trigger',
    'avg_unix',
    'unix_disarm',
    'sd_lat',
    'sd_lng',
    'delta_x',
    'delta_y',
    'trigger_lat',
    'trigger_lng',
    'avg_lat',
    'avg_lng',
    'disarm_lat',
    'disarm_lng',
    'mode_nav_status',
    'obs_nav_status',
    'obs_count',
    'state_initial',
    'state_final',
    'wkt_trigger_pt',
    'wkt_avg_pt',
    'wkt_disarm_pt',
    'wkt_triline',
    'wkt_sd_box',
    'is_valid']

    # Now attempt to populate result
    try:
        # get trigger location and time
        trigger_lat = lat_list[0]
        trigger_lng = lng_list[0]
        trigger_unix= unix_list[0] 

        # get average location and time
        avg_lat = calc_list_average(lat_list)
        avg_lng = calc_list_average(lng_list)
        avg_unix = math.floor(calc_list_average(unix_list))# always want an integer second. use floor    
        
        # get disarm location and time
        disarm_lat = lat_list[-1]
        disarm_lng = lng_list[-1]  
        disarm_unix= unix_list[-1] 
        
        # get mode of navigation status and number of observations in mode
        full_mode_nav_status = calc_list_mode(navigation_status_list,1) # get last value in mode list
        mode_nav_status = full_mode_nav_status[0] # get the mode value
        obs_nav_status = full_mode_nav_status[1] # gets the count of time this is use in this stopped ship event
        
        wkt_trigger_pt = calc_point_wkt(trigger_lat,trigger_lng)
        wkt_avg_pt = calc_point_wkt(avg_lat,avg_lng)
        wkt_disarm_pt = calc_point_wkt(disarm_lat,disarm_lng)
        
        
        
        # get standard deviation in m
        delta_xy = calc_standard_deviation_xy(lat_list, lng_list, min_observations, h3_version)
        delta_x = delta_xy[0]
        delta_y = delta_xy[-1]
        
        # crack out state values from tuple
        state_initial = state_values[0]
        state_final = state_values[-1]
        
        # now deal with lenght issues in derived data on lists
        obs_count = len(lat_list)
        if obs_count >=  min_observations :
            status = 'valid'
            
            wkt_triline = calc_tri_line_wkt(trigger_lat,trigger_lng, avg_lat, avg_lng, disarm_lat, disarm_lng)
            
            # internal helper acting on lat/lng now safe to do standard deviation caluation
            standard_deviation_lat = calc_list_standard_deviation(lat_list)
            standard_deviation_lng = calc_list_standard_deviation(lng_list)
            
            
            wkt_sd_box = calc_stddev_boundingbox_wkt(avg_lat,avg_lng,
            standard_deviation_lat,standard_deviation_lng,
            standard_deviation_factor)
        
        #deal with invalid stopping events, insufficent observations
        # a different method is used to deal with these events since 
        # standard deviation cannot be reliably calculated
        else:
            status = 'invalid'        
            standard_deviation_lat = 0 
            standard_deviation_lng = 0
            
            # We do not want to risk degnerate goemetries
            wkt_triline = 'LINESTRING EMPTY'
            wkt_sd_box = 'POLYGON EMPTY'           
            
        # populate result with trigger data passed in to function    
        result.append(h3_trigger) # h3
        result.append(mmsi) # mmsi
        result.append(imo) # imo
        result.append(ais_length) # ais_length
        result.append(ais_width) # ais_width
        result.append(ais_vessel_type) # ais_vessel_type
        result.append(ais_vessel_type_main) # ais_vessel_type_main
        result.append(ais_obs_time)
        
        # Now append passed in values to function
        result.append(delta_time_lower)
        result.append(delta_time_upper)         
        
        # Now append result with data derived in function        
        result.append(trigger_unix)
        result.append(avg_unix)
        result.append(disarm_unix)
        result.append(standard_deviation_lat)
        result.append(standard_deviation_lng)
        result.append(delta_x)
        result.append(delta_y)
        result.append(trigger_lat)
        result.append(trigger_lng)
        result.append(avg_lat)
        result.append(avg_lng)
        result.append(disarm_lat)
        result.append(disarm_lng)        
        
        # Navigation info
        result.append(mode_nav_status)
        result.append(obs_nav_status) # Number of observations for navigations status
        result.append(obs_count) # Number of observations
        result.append(state_initial) # State of observation stream at start of stopping event
        result.append(state_final) # State of observation stream at end of stopping event
        
        # wkt data
        result.append(wkt_trigger_pt)
        result.append(wkt_avg_pt)
        result.append(wkt_disarm_pt)
        result.append(wkt_triline)
        result.append(wkt_sd_box)
        
        # status
        result.append(status)

        # check if outputs are internally consistent.
        if (len(header_list)!=len(result)):
            raise ValueError("length of internal header list must match length of intended result\n \
                             i.e len(_result_calculator[1]) != len(_result_calculator[2])")
            
        return status, result, header_list
        
    except Exception as e:
        print(f'Result calculation fail...')
        print(f'.....')
        print(f'Error: \n{e}\n') 



def calc_standard_deviation_xy(
    lat_list: List[float], 
    lng_list:List[float] , 
    min_length: int, 
    h3_version: int
    ) -> Tuple[float,float]:
    """
    Gives indication of maginitude of standard deviation of lat/long in terms of m.
    Does this by calcualation of average and standard deviations and purturbing average co-ords
    by standard deviation on eastings and northings and doing great circle caluations between
    average and purturbed co-ordinates.
    
    Implements
    calc_haversine_m
    calc_list_standard_deviation
    calc_list_average
    
    Usage
    calc_standard_deviation_xy(list_lat1 ,list_lng1,5,3 )
    calc_standard_deviation_xy(lat_list=list_lat1, lng_list=list_lng1, min_length=5,h3_version=3 )
    
    
    parameters
    --------------
    lat_list: list[float] - list from stopped ship event of latituidues
    lng_list:list[float]  - list from stopped ship event of longitudes
    min_length: integer - as we are doing calcs of statistical values need a minimum number to give meaningful
    result.  will return (0,0) length of  lists are less than this.
    h3_version: integer {3|4} - version of h3 api used  
    
    
    returns
    --------------
    tuple (x, y)
    
    """
    try:
        if not ((h3_version ==3) or (h3_version ==4)):
            raise ValueError("H3 Version error: h3_version only accepts integer type value of {3|4}")    
        if not (len(lat_list)==len(lng_list)):
            raise ValueError("List Length unmatched error: Both lists must be the same length")
        
        if len(lat_list) < min_length:
            delta_x = 0
            delta_y = 0
            return delta_x, delta_y
        else:
            avg_lat = calc_list_average(lat_list)
            avg_lng = calc_list_average(lng_list)
            sd_lat = calc_list_standard_deviation(lat_list)
            sd_lng = calc_list_standard_deviation(lng_list)
            
            #Following are to deal with edge case of going out of range for haversine calc
            if avg_lat>0:
                delta_lat = avg_lat - sd_lat
            else:
                delta_lat = avg_lat + sd_lat
                
            if avg_lng>0:
                delta_lng = avg_lng - sd_lng
            else:
                delta_lng = avg_lng + sd_lng

            
            # now use haversine to get distance in metres along easting (x) and northing (y)
            delta_x = calc_haversine_m(avg_lat, avg_lng, avg_lat, delta_lng, h3_version)
            delta_y = calc_haversine_m(avg_lat, avg_lng, delta_lat, avg_lng, h3_version)
            return delta_x, delta_y
            
    
    except Exception as e:
        print('calc_standard_deviation_xy failed')
        print(f'Check if have H3 installed and vesion number for H3')
        print(f'Error: \n{e}\n')



def calc_haversine_m(
    lat1: float,
    lng1: float,
    lat2: float,
    lng2: float,
    h3_version: int
    ) -> float:
    """
    Calculates haversine distance between two points defined by lat/lng
    Wrapper around H3 functions:
    version 3: implements h3.point_dist(point1, point2, unit='m')
    version 4: implements h3.latlng_distance(point1, point2, unit='m')   
    

    Usage
    calc_haversine_m(lat1, lng1,lat2,lng2,3)
    calc_haversine_m(lat2= value, lng2= value, lat1=value, lng1=value, h3_version=3|4)
    
    parameters
    --------------    
    lat1: float - decimal degree point1 latitude 
    lng1: float - decimal degree point1 longitude
    lat2: float - decimal degree point2 latitude
    lng2: float - decimal degree point2 longitude
    h3_version: integer {3|4} - version of h3 api used  
 

    returns
    --------------
    float -"great circle" or "haversine" distance between pairs of  points (lat/lng pairs) in meters.
    
    """
    
    try:
        if not ((h3_version ==3) or (h3_version ==4)):
            raise ValueError("h3_version only accepts integer type value of {3|4}")   
        

        if not ( 
        (-90 <= lat1 <= 90) and
        (-90 <= lat2 <= 90)
        ):
            raise ValueError("Haversine Latitude Input error: co-ordinates must be within -90° to 90° for latitude") 
            
        if not ( 
        (-180 <= lng1 <= 180) and
        (-180 <= lng2 <= 180)
        ):
            raise ValueError("Haversine Longitude Input error: co-ordinates must be within -180° to 180° for longitude")         

        
        point1 = (lat1, lng1)
        point2 = (lat2, lng2)
        
        if h3_version==3:
            return h3.point_dist(point1, point2, unit='m')
        elif h3_version==4:
            return h3.latlng_distance(point1, point2, unit='m') 
        else:
            pass
    

    except Exception as e:
        print('calc_haversine_m failed')
        print(f'inputs should be floats for lat/long values?')
        print(f'Check if have vesion number installed')
        
        print(f'Error: \n{e}\n')



def calc_list_mode(lst, result_type = 1):
    '''
    attempts to return the mode of a list
    Robbed from: https://www.geeksforgeeks.org/how-to-calculate-the-mode-of-numpy-array/
    returns this as a list with the mode value and count within input list.
    can also return list of list for case where mode has more than one value, this is rare edge case
    which lead to adding frequency as aim of this is have domininat type of observation.
    If there is more than one mode then by definition its 50/50 at best which call to use.
    Hence can use freaquency count to gague confidence of mode.
    Use case is for determination of stopped ship type: using for categorical values.



    lst - list we want to find mode of
    
    result_type {default = 1} - Could have more than one mode, choose to return the last of the list
        acceeptable values are
        0 - returns the first of many
        1 - returns the last of many
        'all' - returns the full list.

    returns
    -----------
    List: [Mode, frequency]
    or 
    List of List [[Mode, frequency],[Mode, frequency],..,[Mode, frequency]]
    



    '''
    result_types = [1,0,'all']
    # exception for non list input
    if type(lst) is not list:
        raise ValueError(f"Invalid Input, List expected")

    if result_type not in result_types:
        raise ValueError(f"Invalid Input, result type can only be value of 0,1,'all'")
    
    
    try:
        # create a dictionary to hold results as value:count
        freq = {}

        # Iterate the list
        for i in lst:
        
            # mapping each value of list to a
            # dictionary - dict.setdefault: If the key does not exist, insert the key, with the specified value
            # In this case zero
            freq.setdefault(i, 0)
            freq[i] += 1
            
        # finding maximum value of dictionary
        hf = max(freq.values())
        
        # creating an empty list
        hflst = []
        
        # using for loop we are checking for most
        # repeated value
        for i, j in freq.items():
            if j == hf:
                hflst.append([i,j])
                
        # returning the result, we could always have more than 1, defualt returns the last in the list.
        if result_type == 1:
            return hflst[-1]
        elif  result_type == 0:
            return hflst[0]
        else:
            return hflst

    except Exception as e:
        print(f'function requires list')
        print(f'and list can contain any type of values')
        print(f'Error: \n{e}\n')

        

def calc_list_average(lst):
    '''
    Attempts to return the average of a list in python.
    Should all be numeric
    Note this is not wrap around safe -180°|180°...
    Should only be used on stopping points list values
    Not for general purpose use

    '''
    
    try:
        return np.nanmean(lst)
    except Exception as e:
        print(f'function requires list')
        print(f'and list should only contain floats?')
        print(f'Error: \n{e}\n')

def calc_list_standard_deviation(lst):
    '''
    Attempts to return standard deviation of a list of floats
    Should all be numeric.
    Note this is not wrap around safe -180°|180°...
    Use sample standard deviation as cannot know how many are sent
    to be calculated.
    Should only be used on stopping points list values
    Not for general purpose use
    '''
    
    try:
        if len(lst)>1:
            return np.nanstd(lst,ddof=1)
        else:
            return 0
    except Exception as e:
        print(f'function requires list')
        print(f'and list should only contain floats?')
        print(f'Error: \n{e}\n')


def calc_stddev_boundingbox_wkt(lat,lng,lat_sd,lng_sd,factor):
    '''
    Returns well known text (wkt) representation of bounding box based on
    average lat/long standard deviations of observations used to derive that average.
    Size of the box corresponds to factor based on the number of Standard deviations used.
    Purpose is for visulisation and sense checking.
    
    lat <--> Y-Axis
    lng <--> X-Axis    
    Note 
    AIS work in lat/lng ie northings and eastings:
    wkt works on X Y co-ordinates: ie eastings and northings ==> lng and lat    
    ______________UR
    |             |
    |             |
    |_____________|
    LL
    
    A bounding box is defined by two co-ord pairs
    LL - Lower Left
    UP - Upper Right
    
    Parameters
    ----------
    lat= float: latitude of average point 
    lng= float: longitude of average point
    lat_sd= float: standard deviation calculated from _list_standard_deviation
    lng_sd= float: standard deviation calculated from _list_standard_deviation
    factor = multiplication factor applied to lat_sd and lng_sd, i.e. number of standard deviations 
    
    Returns
    ----------
    wkt polygon
    POLYGON((xleft ylow, xleft yupper, xright yupper, xright ylow, xleft ylow ))

    '''
    
    try:
        lng_delta = factor*lng_sd
        xleft = lng-lng_delta
        xright = lng+lng_delta

        lat_delta = factor*lat_sd
        ylow = lat-lat_delta
        yupper= lat+lat_delta

        return f'POLYGON(({xleft} {ylow}, {xleft} {yupper}, {xright} {yupper}, {xright} {ylow}, {xleft} {ylow}))'
    
    except Exception as e:
        print(f'Polygon WKT write fail...')
        print(f'.....')
        print(f'Error: \n{e}\n')


def calc_tri_line_wkt(lat_start,lng_start, lat_mid,lng_mid,lat_end,lng_end):
    '''
    Creates a tri-line of three points as well known text (wkt).
    LINESTRING (30 10, 10 30, 40 40)
    The three points of the tri line correspond to: 
    -trigger event, 
    -average co-ord, 
    -last co-ordinates in trigger event (observation prior to disarm event)
    
    Purpose is for visulisation and sense checking
    
    lat <--> Y-Axis
    lng <--> X-Axis    
    Note 
    AIS work in lat/lng ie northings and eastings:
    wkt works on X Y co-ordinates: ie eastings and northings ==> lng and lat
    
    
    trigger event
    *---------------@ average point
                    |
                    |
                    |
                    |
                    # last co-ordinates in stopping event
    
    '''
    return f'LINESTRING ({lng_start} {lat_start}, {lng_mid} {lat_mid},{lng_end} {lat_end})'

def calc_point_wkt(lat,lng):
    '''
    creates well known text (wkt) representation of point. 
    Interned for internal use so minimak checks of validation applied!
    POINT (30 10)
    Purpose is for visulisation and sense checking
    
    lat <--> Y-Axis
    lng <--> X-Axis    
    Note 
    AIS work in lat/lng ie northings and eastings:
    wkt works on X Y co-ordinates: ie eastings and northings ==> lng and lat 
    '''
    return f'POINT ({lng} {lat})'


def calc_lower_upper_time_estimates(time1, time2, time3, time4, time_factor):
    '''
    name of function: _lower_upper_time_estimates
    hints at order of output results
    
    Put this duplicated calculation into a function from original smbm
    calculate time difference for upper and lower bounds in terms of hours.
    rounds to two decimal places ≈ 36 seconds of resoution - more than adequate.
    delta_time_upper = (the_current[idx_cluster_time] - the_prior[idx_cluster_time])*time_factor
    delta_time_lower = (the_previous[idx_cluster_time] - the_trigger[idx_cluster_time])*time_factor

    returns tuple of 
    (delta_time_lower, delta_time_upper) in terms of hours, more useful for a human to understand.
    
    Use this for any ordered time arrow estimation for series of obsevations
    
    >------1----->--------2----->---------3------>--------4----->
    >-prior_time->-trigger_time->-previous_time-->--escape_time->
    >--time1----->----time2----->-----time3------>----time4----->
    
    Parameters
    ----------    
    time1 - unix time: prior to trigging event
    time2 - unix time: of trigging event
    time3 - unix time: of disarm event (prior to escaping event)
    time4 - unix time: of escaping event 

    time_factor {-1|1}: depending on your arrow of time.
        Negative value for decending sort used on time
        Positive value for ascending sort use on time.
    
    Return
    ----------
    tuple - (delta_time_lower, delta_time_upper)
    
    '''
    try:
        if not (time_factor == 1 or time_factor == -1):
            raise ValueError("only time_factor of 1 or -1 allowed")
        delta_time_upper = int(round(time_factor*(time4-time1)/3600))
        delta_time_lower = int(round(time_factor*(time3-time2)/3600))
        return delta_time_lower, delta_time_upper
        
    except Exception as e:
        print(f'Time calculation fail...')
        print(f'.....')
        print(f'Error: \n{e}\n') 
