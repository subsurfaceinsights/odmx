#!/usr/bin/env python3

"""
Module to house an abstract data source class.
"""

from abc import ABC, abstractmethod


class DataSource(ABC):
    """
    Abstract class for a given datasource entry.
    """
    @abstractmethod
    def __init__(self, project_name, project_path, data_path):
        """
        All data sources will have a project name, project path, and data path.
        The project name is the name of the project, the project path is the
        path to the project definition directory, which includes the JSONs
        describing the project and data mappings. The data path is the path
        to the data directory, which includes the data files that are written
        in the harvest stage and read in the ingest stage.

        If there are additional parameters that are needed for a specific
        data source, they can be added in the subclass's constructor.
        The data_source.json file should have a shared_info key which
        corresponds to the parameters that are needed for all stages,
        which is set here in
        __init__

        Common shared parameters include site identifiers for pulling data,
        or paths to CSV files that are relative to the data_path directory for
        harvesting or ingesting data.
        """

    @abstractmethod
    def harvest(self):
        """
        Placeholder abstract method for harvesting data. The harvest stage is
        for gathering data from its source and saving it to the local disk
        under the data directory. The data should be saved in a format that is
        easy to ingest in the ingest stage.

        If a subclass specifies additional parameters in its constructor, then
        they should be set in the harvesting_info key of the data_source.json
        entry for that data source.

        Common additional parameters include a path to authentication
        credentials for accessing the data source, special parameters for
        pulling data, etc.

        If data already exists in the data directory, this method should handle
        updating the data in the data directory. Sometimes this stage is not
        necessary if the data on the disk is statically provided.
        """

    @abstractmethod
    def ingest(self, odmx_db_con, feeder_db_con):
        """
        Placeholder abstract method for ingesting data. The ingest stage is
        for reading in data from the local disk and writing it to the feeder
        database. The data should be read in from the data directory and then
        written to the feeder database with minimal processing.

        If there are additional parameters that are needed for a specific
        data source, they can be added in the subclass's constructor.
        The data_source.json file should have a ingest_info key which
        corresponds to the parameters that are needed for the ingest stage
        """

    @abstractmethod
    def process(self, odmx_db_con, feeder_db_con):
        """
        Placeholder abstract method for processing data.
        The process stage is for reading in data from the feeder database and
        adding it to the ODMX database. The datastream processing involves
        creating views against the feeder database and then materializing those
        views into the ODMX database.

        If there are additional parameters that are needed for a specific
        data source, they can be added in the subclass's constructor.
        The data_source.json file should have a process_info key which
        corresponds to the parameters that are needed for the process stage

        A common additional parameter is the associated sampling feature code
        """
