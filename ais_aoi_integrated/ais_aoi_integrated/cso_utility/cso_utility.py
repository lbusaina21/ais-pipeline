"""
justin.mcgurk@cso.ie 
October 2022 
Central Statistics Office - Ireland 
AIS project.

Utlility functions module

A single leading underscore in front of a variable, a function, or a method name means that these
objects are used internally. This is more of a syntax hint to the programmer and is not enforced
by the Python interpreter which means that these objects can still be accessed in one way on 
another from another script. 
(https://towardsdatascience.com/whats-the-meaning-of-single-and-double-underscores-in-python-3d27d57d6bd1)

Using this pattern to allow for addition functions to be added in later with out needing to change base code via new modules

Implements
_utility : 
    cso_wkt_load - utility to create geometry from wkt field in data frame.
    cso_dataframe_stripper - utility function to return only columns of a dataframe supplied in list

_calc :
    calc_list_mode - returns mode (most common value) of a list
    cso_list_average - returns average (mean) of a list of numbers
    cso_list_standard_deviation - returns standard deviation of a list of numbers, returns 0 if list is of length=1
    cso_lower_upper_time_estimates - returns tuple of lower and upper time estimates (in hours)  from  four unix input times.

    cso_stddev_boundingbox_wkt - returns polygon based on a average and standard deviation data as well known text 
    cso_tri_line_wkt - returns polyline based on three input co-ordinates as well known text
    cso_point_wkt - returns point as well known text from co-ordinates.
"""

from ._utility import *
from ._calc import *
