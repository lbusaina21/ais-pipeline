from setuptools import setup, find_packages

setup(
    name="ais_aoi_integrated",
    version="0.1.9",
    description="Utilities for working with Indonesian AIS and geospatial H3 data",
    author="Gery Nastiar",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.3.0",
        "pytest",
        "geopandas",
        "shapely",
        "folium",
        "h3==3.7.7",
        "sedona",
        "apache-sedona",
        "spark",
        "IPython"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)
