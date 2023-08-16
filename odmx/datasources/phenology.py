#!/usr/bin/env python3

"""
Module for LBNLSFA phenology data harvesting, ingestion, and processing.
"""

from odmx.datasources.ssi.gradient import GradientDataSource


class PhenologyDataSource(GradientDataSource):
    """
    This data is identical to LBNLSFA gradient data, and so we simply use that
    module's class.
    """
