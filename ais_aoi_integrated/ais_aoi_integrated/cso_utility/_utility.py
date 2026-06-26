"""
justin.mcgurk@cso.ie 
October 2022 
Central Statistics Office - Ireland 
AIS project.

Utlility functions

cso_wkt_load - utility to create geometry from wkt field in data frame.
    version_2 of this function, now implements optional drop of wkt field
    Taken from code refactored for UNECE-ML-2022 use case
    

Borrows from: based on: https://github.com/datasciencecampus/UNGP-AIS-ETL/blob/feature/StructuredStreaming/spark_etl_bench/src/utils.py 
from AIS Task Team > ais > _aisfilter

cso_dataframe_stripper - utility function to return only columns of a dataframe supplied in list
Borrows from: _aisfilter from https://code.officialstatistics.org/trade-task-team-phase-1/ais

#Sedona Imports
import sedona.sql
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.core.SpatialRDD import PolygonRDD, PointRDD
from sedona.core.enums import FileDataSplitter


# Pyspark Imports
#import pyspark.sql.functions as psf
import pyspark.sql.functions as F
import pyspark.sql.types as pst
from pyspark import StorageLevel
from pyspark.sql import SparkSession
"""

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from typing import Set, Dict, List, Tuple, Optional
import logging


def cso_wkt_load(
    spark: SparkSession,
    df: DataFrame, 
    wkt_field = 'WKT_geom',
    drop_wkt = True )-> DataFrame:
    '''
    Implement Sedona to make a data frame with a geometary object
    https://sedona.apache.org/tutorial/core-python/ 
    create a temp view that implements ST_GeomFromWKT() function.
    
    Tabular data that contains a field that has a wkt field that is going to be used to
    create a geometry object with field name geom and source string removed as no longer needed
    once geometry is created from it.
    Script from CSO-Ireland (_utility.cso_wkt_load())and modified for use in this project.
    
    sample usage:
    
    Parameters
    ----------
    spark: SparkSession

    df: spark data frame

    wkt_field: String: Field name in df that contain wkt geometry string used for geometry creation.
        Removed from output by default
        default: WKT_geom
    
    drop_wkt: Boolean: Gives user option to drop wkt_field once consumed in geometry creation.
        default: True --> Field wkt_field will be removed from output
        False --> Field wkt_field will NOT be removed from output
    
    Returns
    -------
    spark data frame
        geometry object created in to new field: geom
    '''
    try:
        df.createOrReplaceTempView("x")
        geom_df= spark.sql(
        rf'''
        SELECT 
        *, 
        ST_GeomFromWKT({wkt_field}) as geom  
        
        FROM x

        '''
        )

        if drop_wkt:
            geom_df = geom_df.drop(rf'{wkt_field}')
        
        geom_df.printSchema()
        print(geom_df.head(1))  #Use this to throw error for bad geometry types
        
        return geom_df
    
    except Exception as e:
        print(f'Function requires pyspark.sql.dataframe.DataFrame object as input \
        \nCheck wkt field is well formed? \
        \nCheck field choice is wkt data?')
        print(f'Error: \n{e}\n')

def cso_dataframe_stripper(
    spark: SparkSession, 
    df: DataFrame, 
    keep_list: Optional[List[str]] = ["*"]
    ) -> DataFrame:
    """
    Utility to return only the columns listed by user as input.  
    This will do exactly what it says! It is the user responsibility to ensure this is what they want to do.
    Does not mutate the input dataframe and silently ingnores 
    
    Firstly gets list of columns in data frame as a list
    Secondly, removes specified columns to create remainder list
    Thirdly, applies .drop() to remainder list.

    This is to allow for some error input.  Recommend using df.printSchema() prior to check this is what 
    user wants to do.

    Parameters
    ----------
    spark: SparkSession

    df: spark data frame

    keep_list: list of str, default ["*"]
        the list of columns to keep. If not supplied, all columns are returned
    
    Returns
    -------
    Spark dataframe with the colums specified if they exist in the input dataframe.    
    """
    try:
        # 1. get list of colums in input data frame, this will be mutated.
        col_list = df.columns
        #col_list = df.schema.names

        # 2. Remove elements from col_list
        if keep_list == ["*"]:
            pass
        else:
            for k in keep_list:
                # deal with duplicates
                while k in col_list:
                    col_list.remove(k)

        # 3. Now of list of colums that does not include specified so drop them all!
        
        df.createOrReplaceTempView("x")
        return_df = spark.sql(
        rf'''
        select *
        from x
        '''
        )   
        
        # Only drop if we have
        if keep_list != ["*"]:
            return_df = return_df.drop(*col_list)
        
        # User feed back
        return_df.printSchema()

        return return_df
    
    except Exception as e:
        print(f'Function requires pyspark.sql.dataframe.DataFrame object as input')
        print(f'Check keep_list contains only strings\nError is.....')
        print(f'Error: \n{e}\n')        
