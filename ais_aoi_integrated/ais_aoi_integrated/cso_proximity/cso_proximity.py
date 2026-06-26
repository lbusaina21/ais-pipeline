"""
justin.mcgurk@cso.ie 
July 2022 
Central Statistics Office - Ireland 
AIS project.

Utlility functions module to test proximity in terms of h3 indices

A single leading underscore in front of a variable, a function, or a method name means that these
objects are used internally. This is more of a syntax hint to the programmer and is not enforced
by the Python interpreter which means that these objects can still be accessed in one way on 
another from another script. 
(https://towardsdatascience.com/whats-the-meaning-of-single-and-double-underscores-in-python-3d27d57d6bd1)

Using this pattern to allow for addition functions to be added in later with out needing to change base code via new modules

Implements
_proximity : 
    cso_k_ring - gets set of H3 indices adjacent to a given h3 index
    cso_h3_adjacency_test - tests if a h3 index is a member of a set of h3 indices output from cso_k_ring

    For the following thin wrappers around h3 see - https://h3geo.org/docs/api/inspection
    cso_resolution_test - tests in inputs are in terms of same H3 resolution.
    cso_string_to_h3 - Wrapper around h3.h3_to_string
    cso_h3_to_string - Wrapper around h3.string_to_h3

    get_h3_dist- Provides the distance in grid cells between the two points.
    get_h3_centroid - Returns an indicative lat long for a given H3Index
    get_h3_index - converts a lat long value into h3 index value in terms of u_64 bit integer as used within UNGP AIS ecosystem.
        version 3 : wrapper around h3.geo_to_h3(lat, lng, resolution)
        version 4 : wrapper around h3.latlng_to_cell(lat, lng, resolution)



"""

from ._proximity import *
