"""
justin.mcgurk@cso.ie 
July 2022 
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
_ais : 
    cso_ais2geom - create points from ais data
    cso_ais2areas - does spatial join on ais to area data (generally ports)
    cso_ais2unixtime - gives a unix time for sorting    


"""

from ._ais import *
