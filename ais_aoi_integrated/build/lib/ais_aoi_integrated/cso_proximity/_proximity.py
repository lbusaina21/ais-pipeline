"""
justin.mcgurk@cso.ie 
October 2022 
Central Statistics Office - Ireland 
AIS project.

Utlility functions module to test proximity in terms of h3 indices

cso_k_ring - gets set of H3 incdices adjacent to a given h3 intex
cso_h3_adjacency_test - tests if a h3 index is a member of a set of h3 indices output from cso_k_ring

For the following thin wrappers around h3 see - https://h3geo.org/docs/api/inspection
cso_resolution_test - tests in inputs are in terms of same H3 resolution.
cso_string_to_h3 - Wrapper around h3.h3_to_string
cso_h3_to_string - Wrapper around h3.string_to_h3

get_h3_dist- Provides the distance in grid cells between the two points.
get_h3_centroid - Returns an indicative lat long for a given H3Index
get_h3_index - converts a lat long value into h3 index value in terms of u_64 bit integer
    as used within UNGP AIS ecosystem.
    version 3 : wrapper around h3.geo_to_h3(lat, lng, resolution)
    version 4 : wrapper around h3.latlng_to_cell(lat, lng, resolution)


From https://h3geo.org/docs/core-library/h3Indexing
H3Index Representation
An H3Index is the integer representation of an H3 index, which may be one of multiple modes to
indicate the concept being indexed.
H3Index Representation
The canonical string representation of an H3Index is the hexadecimal representation of the integer,
using lowercase letters. The string representation is variable length (no zero padding) and is not
prefixed or suffixed.

Examples:
h3_index_uint_t : 621922022744817663
h3 index_string : '8a182c8dbb27fff'
"""


# get our required imports

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

# h3 imports
import h3
import h3.api.numpy_int as h3int

def cso_k_ring(h3_index_uint_t, k=1):
    """
    Wrapper aroud h3.k_ring(origin, k).
    Must have imported h3 prior to using this.
    
    Pass in h3 index as 64 bit integer and get a set of adjacent H3 indexes to depth k of same resolution
    k-rings produces indices within k distance of the origin index.
    see: https://h3geo.org/docs/api/traversal#kring
    utility to use h3 values from ais data and get set of adjacent h3 indices to k depth at same reolution.

    k-ring 0 is defined as the origin index, k-ring 1 is defined as k-ring 0 and all
    neighboring indices, and so on.
    Use this to create a neighbourhood to be tested against.
    
    Parameters
    ----------
    h3_index_uint_t: integer (64bit)
        value from AIS data h3_index_uint_t that AIS ping is in
    k: integer: default =1
    
    Returns
    ----------
    set
        Output is placed in the provided array in no particular order. 
        Elements of the output array may be left zero, as can happen when crossing a pentagon.
    
    """
    try:
        return h3.k_ring(h3.h3_to_string(h3_index_uint_t), k)
    except Exception as e:
        print(f'Function requires H3 index as 64 bit integer?')
        print(f'k should be integer greater than 0 and reasonably small integer?')
        print(f'Error: \n{e}\n')


def cso_h3_adjacency_test(h3_index_uint_t,kring):
    """
    h3_adjacency test.  Test if a h3 index is in a given k-ring.
    Note the h3 resolution of h3_index_uint_t must be in terms of the k-ring set resolution
    
    Takes value from AIS data for value of h3_index_uint_t.
    Use this to evaluate if an observation is in a neighborhood.
    
    Parameters
    ----------
    h3_index_uint_t: integer (64bit)
        value from AIS data H3_int_index_n that AIS ping is in
    kring: set
        we are testing if h3_index_uint_t is member of this set of indices that are
        output from cso_k_ring().
     
    Returns
    ----------
    boolean
    
    """
    try:
        index = h3.h3_to_string(h3_index_uint_t)
        return index in kring
    except Exception as e:
        print(f'K-Ring testing has failed?')
        print(f'Error: \n{e}\n')


def cso_resolution_test(h3_index_uint_t,kring):
    """
    Tests if inputs are in terms of same H3 resolution. Wrapper around h3 function
    h3_get_resolution.  Gets first value from kring set for comparison.
    Silently deals with different representations of H3Index. 
    
    Parameters
    ----------
    h3_index_uint_t: integer (64bit)
        value from AIS data
    kring: set
        we are testing if h3_index_uint_t is of the same resolution.
        output of cso_k_ring

    Returns
    ----------
    boolean

    """
    try:
        res1 = h3.h3_get_resolution(h3.h3_to_string(h3_index_uint_t))
        res2 = h3.h3_get_resolution(next(iter(kring)))
        result = False
        if res1==res2:
            result =  True
        return result
    except Exception as e:
        print(f'cso_resolution_test has failed?')
        print(f'Error: \n{e}\n')

def cso_string_to_h3(h):
    """
    Wrapper around h3.string_to_h3(h)
    https://h3geo.org/docs/api/inspection
    Converts the string representation to H3Index (uint64_t) representation.

    Parameters
    ----------
    h : string
        canonical string representation of an H3Index is the hexadecimal representation of the integer, using lowercase letters.

    Returns
    ----------
    uint64_t: representation of H3Index.  0 on error
    """
    try:
        return h3.string_to_h3(h)
    except Exception as e:
        print(f'cso_string_to_h3 has failed?')
        print(f'Error: \n{e}\n')


def cso_h3_to_string(h):
    """
    Wrapper around h3.h3_to_string(h)
    https://h3geo.org/docs/api/inspection
    Converts the H3Index representation of the index to the string representation.
    canonical string representation of an H3Index is the hexadecimal representation of the integer, using lowercase letters.

    Parameters
    ----------
    h : uint64_t
        representation of H3Index

    Returns
    string
        canonical string representation of an H3Index using lowercase letters.
    ----------


    """
    try:
        return h3.h3_to_string(h)
    except Exception as e:
        print(f'cso_string_to_h3 has failed?')
        print(f'Error: \n{e}\n')


def get_h3_index(lat,lng,resolution,version=3):
    """
    version 3 : wrapper around h3.geo_to_h3(lat, lng, resolution)
    version 4 : wrapper around h3.latlng_to_cell(lat, lng, resolution)
    
    converts a lat long value into h3 index value in terms of u_64 bit integer
    as used within UNGP AIS ecosystem.
    i.e. it gets the H3 index that the point is within for a given resolution level.
    
    Note
    
    Parameters
    ----------    
    lat: float - point latitude in decimal degrees
    long: float - point longitude in decimal degrees
    resolution: integer  value 0-15 expected 
    version: Integer-default 3 - Version of H3 in use.  
        currently this version 3 on ungp, however this may change without notice.
        version 4 introduced breaking changes
    
    Returns    
    ----------  
    uint64_t: representation of H3Index.  0 on error

    """
    try:
        if version==3:
            return h3.string_to_h3(h3.geo_to_h3(lat, lng, resolution))
        else:
            return h3.string_to_h3(h3.latlng_to_cell(lat, lng, resolution))
    except Exception as e:
        print(f'Function lat long as decimal degrees?')
        print(f'resolution should be integer greater in range 0-15?')
        print(f'Error: \n{e}\n')


def get_h3_centroid(H3Index,  version=3):
    """
    Returns an indicative lat long for a given H3Index
    version 3 : wrapper around h3.h3_to_geo(h3)
    version 4 : wrapper around h3.cellToLatLng(h3)
    
    Finds the center of the cell in grid space.  Silently deals with H3 preference
    for string representation and UNGP preference for integer representation.
  
    Converts a  h3 index value into  terms of u_64 bit integer
    as used within UNGP AIS ecosystem.
    i.e. it gets the H3 index that the point is within for a given resolution level.
    
    The center will drift versus the centroid of the cell on Earth 
    due to distortion from the gnomonic projection within the 
    icosahedron face it resides on and its distance from 
    the center of the icosahedron face.
    For details of gnomonic projection see: 
    https://mathworld.wolfram.com/GnomonicProjection.html
    https://pro.arcgis.com/en/pro-app/latest/help/mapping/properties/gnomonic.htm
    
    Parameters
    ----------
    H3Index: uint64_t - representation of H3Index as used in UNGP
    
    Returns    
    ----------  
    tuple - (latitude,longitude)
    """
    
    try:
        if version==3:
            return(h3.h3_to_geo(h3.h3_to_string(H3Index)))
        else:
            return(h3.cellToLatLng(cso_h3_to_string(H3Index)))
    except Exception as e:
        print(f'Function expected 64bit integer input for cell?')
        print(f'.....')
        print(f'Error: \n{e}\n')


def get_h3_dist(lat1,lng1,lat2,lng2,resolution, version=3):
    '''
    Provides the distance in grid cells between the two points.
    Only tested on v3 of h3.
    Wrapper around H3 functions:
    gets the h3 index of lat/long and uses these for a distance getter
    
    
    version 3: implements h3.geo_to_h3 and h3.h3_distance
    version 4: implements h3.latlng_to_cell  and h3.grid_distance
    
    parameters
    --------------
    lat1: float - decimal degree point1 latitude
    lat1: float - decimal degree point1 longitude
    lat1: float - decimal degree point2 latitude
    lat1: float - decimal degree point2 longitude
    resolution:integer: range 0-15: resolution level of h3 index
    version: integer {3|4}:default3 - version of h3 api used    
    
    returns
    --------------
    integer - version 3: Returns a negative number if finding the distance failed.
        Finding the distance can fail because the two indexes are not comparable
        (different resolutions), too far apart, or are separated by pentagonal distortion.
        This is the same set of limitations as the local IJ coordinate space functions.
    integer - version 4: or an error if finding the distance failed.
        Finding the distance can fail because the two indexes are not comparable
        (different resolutions), too far apart, or are separated by pentagonal distortion.
        This is the same set of limitations as the local IJ coordinate space functions.
    '''

    try:
        if not ((version ==3) or (version ==4)):
            raise ValueError("version only accepts integer type value of {3|4}")
        if resolution not in range(0,16):
            raise ValueError("resolution intger type expected value 0 to 15 expected")        
        if version==3:
            return h3.h3_distance(h3.geo_to_h3(lat1, lng1, resolution),h3.geo_to_h3(lat2, lng2, resolution))
        else:
            return h3.grid_distance(h3.latlng_to_cell(lat1, lng1, resolution),h3.latlng_to_cell(lat2, lng2, resolution))
    
    except Exception as e:
        print(f'inputs should be floats for lat/long values?')
        print(f'version only accepts integer type value of {3|4}?')
        print(f'Error: \n{e}\n')
