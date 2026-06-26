'''
justin.mcgurk@cso.ie 
July 2022 
Central Statistics Office - Ireland 
AIS project.

Utlility functions for processing AIS data, to create points, do spatial join to ports(area objects)

cso_df_join - joins two data frame on field value, options of inner and outer join: intended to replace cso_ais2ships & cso_ais0ships
cso_ais2geom - create points from ais data
cso_ais2areas - does spatial join on ais to area data (generally ports)
cso_ais2unixtime - gives a unix time for sorting

cso_ais2ships - links ais data to ships data - now depreciated, use cso_df_join
cso_ais0ships - ais data with no corresponding ships data - now depreciated, use cso_df_join



cso_ship_type_port_data - unique ship, name, type and ports combinations within  ais data as tuple of tuples


cso_temporal_ship - filters ais data to a ship and sorts on unix time asending

these will all require a spark session

'''

# get our required imports
from datetime import datetime
from typing import Set, Dict, List, Tuple, Optional, Text
import logging


from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F


from ais_aoi_integrated.cso_utility import cso_utility as cso_u

# new to deal with issues
#Sedona Imports
import sedona.sql
#from sedona.register import SedonaRegistrator
#from sedona.utils import SedonaKryoRegistrator, KryoSerializer
#from sedona.core.SpatialRDD import PolygonRDD, PointRDD
#from sedona.core.enums import FileDataSplitter


def cso_df_join(spark: SparkSession, 
    df_left: DataFrame, 
    df_right: DataFrame,
    field_left: Text ,
    field_right: Text,
    join_type: Optional[str]= 'INNER',
    return_type:Optional[str]= 'NullsOnly'
    )-> DataFrame:
    """
    Wrapper around some sql to join two data frames.
    Provides for option to do inner and outer joins
    To allow for choice on usin mmsi and imo number in Join and to partition data
    into mutually exclusive sets.
    
    Parameters
    ----------
    spark: SparkSession
    
    df_left: dataframe on left of join
    
    df_right: dataframe on right of join
    
    field_left: string, column name to use on left of join from df_left
    
    field_right: string, column name to use on right of join from df_right
    
    join_type: string: clause for join type 
        expected values are ('INNER', 'LEFT' )
        Type of Join implemented
    
    return_type: string: Controls Clause for values to return applies only to outer join
        could only want null values which is to do something similar as cso_ais0ships without the 
        inbuilt stripping encoded within cso_ais0ships.
        expected values are ('NullsOnly','All')
        NullsOnly --> restriction to returned values where only records unmatched on Right table are reurned.
        All --> All records from LEFT Join are returned
    
    Returns
    -------
    Spark dataframe of joined data with all input colums returned.     
    """
    return_clause = ""
    join_types = ['INNER', 'LEFT']
    
    return_types = ['NullsOnly','All']
    
    if join_type not in join_types:
        raise ValueError(f"Invalid join type. Expected one of: {join_types}" )
        
    if return_type not in return_types:
        raise ValueError(f"Invalid return type. Expected one of: {return_types}" )
    try:
        # create clause for case of outer and nulls--> similar to cso_ais0ships
        if join_type == 'LEFT' and return_type == 'NullsOnly':
            return_clause = f'WHERE b.{field_right} IS Null'
            
        df_left.createOrReplaceTempView("a")
        df_right.createOrReplaceTempView("b")
        v = spark.sql(
            f'''
            SELECT
                a.*,
                b.*
            FROM
                a {join_type} JOIN b ON a.{field_left} = b.{field_right} 
            {return_clause}
            '''
        )
        return v
    
    except Exception as e:
        print(f'Function cso_df_join has gone bad')
        print(f'Error: \n{e}\n')


def cso_ais2geom(spark, df, x_field='longitude',y_field='latitude', llx= -12.0,lly= 51.1,urx =-5.5,ury = 55.6):
    """
    Creates spatial object from AIS data that allows for down stream spatial processing.
    Have a data frame of AIS data from UN Global platform, does filter on 'Irish Box' with 
    default parameters supplied for Irish Context. 
    Returns a data frame with point spatial object: geom.
    
    Parameters
    ----------
    spark: SparkSession

    df: spark data frame: 
        out put from ais.get_ais() function.

    x_field: string: (default- 'longitude') 
        field in df that correspoinds to longitude value

    y_field: string: (default- 'latitude')
        field in df that correspoinds to latitude value

    llx: float: (default -12.0)  
        Lower left long

    lly: float: (default 51.1) 
        Lower left lat

    urx: float: (default -5.5)
        Upper Right long

    ury: float: (default 55.6)
        Upper Right lat
    
    Returns
    ----------
    spark data frame with point spatial object added: geom
    
    """
    try:
        df.createOrReplaceTempView("a")
        geo_df = spark.sql(
        rf'''
        select 
            *,

            -- now create a point geometry using ST_Point(x,y) x==>long,y==>lat
            ST_Point({x_field},{y_field}) as geom

        from a 

        where 
            -- lower left, x and y must be greater than this
            longitude > {llx} AND 
            latitude > {lly} AND 
            
            -- upper right x and y must be less than this
            longitude < {urx} AND 
            latitude < {ury};
        '''
        )
        return geo_df
    
    except Exception as e:
        print(f'Function requires pyspark.sql.dataframe.DataFrame object as inputs\nCheck x-Check schemas')
        print(f'Error: \n{e}\n')


def cso_ais2areas(spark, aisGeoms,areaGeoms):
    '''
    thin wrapper around spatial join
    Combines attribute data.  We know we have a common filed for geometry call geom
    we strip this out then rename a field internally named geometry as geom and pass 
    result out.
    Needs refactoring to remove this majic vaLUE....
    
    Parameters
    ----------
    spark: SparkSession

    aisGeoms: spark data frame:
        AIS data that has point geometry - output of cso_ais2geom

    areaGeoms: spark data frame
        Area polygons - Generally expected to be port polygons - output of cso_wkt_load expected input
   
    Returns
    ----------
    spark data frame
        point object-Spatial Joined with attribution of port areas point intersects

    '''
    try:
        aisGeoms.createOrReplaceTempView("b")
        areaGeoms.createOrReplaceTempView("p")
        v = spark.sql(
            '''
            SELECT 
            p.*,
            b.*,
            
            b.geom as geometry
            from b,p

            WHERE ST_Intersects(b.geom,p.geom)
            
            
            '''
        )
        # drop duplicazte geom fields and then rename geometry to geom
        # https://sparkbyexamples.com/spark/spark-drop-column-from-dataframe-dataset/
        # https://sparkbyexamples.com/spark/rename-a-column-on-spark-dataframes/
        v = v.drop("geom")
        v = v.withColumnRenamed("geometry","geom")
        return v
        
    except Exception as e:
        print(f'Function requires pyspark.sql.dataframe.DataFrame object as inputs\nCheck x-Check schemas')
        print(f'Do both have geometries? Is point first input and polygons second input?')
        print(f'Error: \n{e}\n')


def cso_ais2unixtime(spark, ais,  ts_field = 'dt_pos_utc'):
    """
    Create a new attribute, cluster_time.
    using ais data as input and timestamp filed we are going to order obsevations on.
        
    timestamp converted to unix time via
       cluster_time = unix_timestamp(a.{ts_field})
    
    Parameters
    ----------
    spark: SparkSession

    ais: spark data frame
    ts_field: string: field name of time stamp to be converted to unix time value
    
    Returns
    ----------
    pyspark.sql.dataframe.DataFrame
        ais with additional field added cluster_time
    """
    try:
        ais.createOrReplaceTempView("a")
        v = spark.sql(
            f'''
            SELECT
                unix_timestamp(a.{ts_field}) as cluster_time,
                a.*
            FROM
                a
            '''
        )
        return v

    except Exception as e:
        print(f'Function requires pyspark.sql.dataframe.DataFrame object as input\nCheck ts_field is timestamp?')
        print(f'Error: \n{e}\n')


    

# cso_ais2ships - links ais data to ships data
def cso_ais2ships(spark, ais, ship, ais_mmsi='mmsi', ship_mmsi='MaritimeMobileServiceIdentityMMSINumber' ):
    """
    Joins ship register data to AIS via inner join on mmsi number
    All data from ais is retained.
    from ship only the following is retained:
        ShipStatus, 
        ShipName,  
        ShiptypeLevel5 
    
    Defaults are supplied for ais_mmsi and ship_mmsi to suit CSO AIS project
    However this is not to say it will aways be the same
    Injects ais_mmsi and ship_mmsi into SQL via string interpolation

    Parameters
    ----------
    spark: SparkSession

    ais: pyspark.sql.dataframe.DataFrame
        dataframe of ais data
    
    ship: pyspark.sql.dataframe.DataFrame
        dataframe of ship register data data

    ais_mmsi: string - default 'mmsi'
        field name of mmsi data we do the join on in ais

    ship_mmsi: string - default 'MaritimeMobileServiceIdentityMMSINumber'
        field name of mmsi data we do the join on in ship

    Return
    ----------
    pyspark.sql.dataframe.DataFrame


    """
    try:
        ais.createOrReplaceTempView("a")
        ship.createOrReplaceTempView("s")
        v = spark.sql(
            f'''
            SELECT
                s.ShipStatus,
                s.ShipName,
                s.ShiptypeLevel5,
                a.*
            FROM
                s INNER JOIN a ON s.{ship_mmsi} = a.{ais_mmsi}
            '''
        )
        return v
    except Exception as e:
        print(f'Function requires pyspark.sql.dataframe.DataFrame object as inputs')
        print(f'Error: \n{e}\n')    


def cso_ais0ships(spark, ais, ship, 
    ais_mmsi='mmsi', 
    ship_mmsi='MaritimeMobileServiceIdentityMMSINumber'):
    """
    Identifies ais data with no-corresponding ships info based on mmsi number
    Does a left outer join and pushes out these null cases and only retrns data from ais.

    Parameters
    ----------
    spark: SparkSession

    ais: pyspark.sql.dataframe.DataFrame
        dataframe of ais data
    
    ship: pyspark.sql.dataframe.DataFrame
        dataframe of ship register data data

    ais_mmsi: string - default 'mmsi'
        field name of mmsi data we do the join on in ais

    ship_mmsi: string - default 'MaritimeMobileServiceIdentityMMSINumber'
        field name of mmsi data we do the join on in ship


    Return
    ----------
    pyspark.sql.dataframe.DataFrame - should be same schema as input ais
    """
    try:
        v = spark.sql(
            f'''
            SELECT b.*
            FROM
            (SELECT
                s.{ship_mmsi},
                a.*
            FROM
                a LEFT JOIN s ON a.{ais_mmsi} = s.{ship_mmsi}) AS b
            WHERE b.{ship_mmsi} IS Null
            '''
        )
        v = v.drop(f"{ship_mmsi}")
        return v 
    except Exception as e:
        print(f'Function requires pyspark.sql.dataframe.DataFrame object as inputs')
        print(f'Error: \n{e}\n')    

def cso_ship_type_port_data(spark,df, mmsi = "mmsi",ship_name= "ShipName",ship_type= "ShiptypeLevel5",port_id = "port_name"):
    """
    Returns the combinations of ais data for unique ship, name, type and ports within extracted ais data as tuple of typles.
    Input AIS data should have gone through cso_ais2areas, cso_ais2ships and ideally cso_ais2unixtime
    
    Parameters
    ----------
    spark: SparkSession

    df - pyspark.sql.dataframe.DataFrame
        ais data has passed through the following processes
            cso_ais2geom
            cso_ais2areas
            cso_ais2ships
            cso_ais2unixtime
        
    mmsi - string {default:"mmsi"}
        field name that contains data holding ship mmsi

    ship_name - string {default: "ShipName"}
        field name that contains data holding ship name

    ship_type - string {default: "ShiptypeLevel5"}
        field name that contains data holding ship type from ships register

    port_id- string {default: "port_name"}
        field name that contains data holding port identifyer

    
    Return
    -----------------
    tuple- tuple of tuples, each sub-tuple t[x] contain four peices of data
        t[x][0] - mmsi data
        t[x][1] - ship name data
        t[x][2] - ship type data
        t[x][3] - port idenfifyer data.
    """
    try:
        iterationList = []
        df.createOrReplaceTempView("a")
        shipPorts = spark.sql(
        f'''
        select distinct         
            a.{mmsi},
            a.{ship_name},
            a.{ship_type},
            a.{port_id}
        FROM
            a 
        '''
        )

        rows_loop =shipPorts.select(mmsi,ship_name,ship_type,port_id).collect()
        iterationList = []
        for rows in rows_loop:
            #print(rows[0], rows[1],rows[2],rows[3])
            iterationList.append((rows[0], rows[1],rows[2],rows[3]))
            
        if len(iterationList) != 0:
            return tuple(iterationList)
        else:
            return None
    except Exception as e:
        print(f'Function to get combinations of ais data for unique ship, name, type and ports has failed.')
        print(f'Error: \n{e}\n')
