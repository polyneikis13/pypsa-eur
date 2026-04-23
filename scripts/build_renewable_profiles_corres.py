# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT
"""
Calculates for each clustered region the (i) installable capacity (based on
land-use from :mod:`determine_availability_matrix`), (ii) the available
generation time series (based on weather data), and (iii) the average distance
from the node for onshore wind, AC-connected offshore wind, DC-connected
offshore wind and solar PV generators.

.. note:: Hydroelectric profiles are built in script :mod:`build_hydro_profiles`.

Outputs
-------

- ``resources/profile_{technology}.nc`` with the following structure

    ===================  ====================  =========================================================
    Field                Dimensions            Description
    ===================  ====================  =========================================================
    profile              year, bus, bin, time  the per unit hourly availability factors for each bus
    -------------------  --------------------  ---------------------------------------------------------
    ===================  ====================  =========================================================

    - **profile**


Description
-----------


"""

import logging
import numpy as np
from dask.distributed import Client
from scripts._helpers import (
    configure_logging,
)


import yaml
import importlib.resources as pkg_resources
from pathlib import Path
from corres.api_pipeline import run_pipeline

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    if "snakemake" not in globals():
        from scripts._helpers import mock_snakemake

        snakemake = mock_snakemake(
            "build_renewable_profiles", clusters=38, technology="offwind-ac"
        )
    configure_logging(snakemake)

    technology = snakemake.wildcards.technology
    clusters = snakemake.wildcards.clusters
    params = snakemake.params.renewable[technology]

    capacity_per_sqkm = params["capacity_per_sqkm"]
    nbins = params.get("resource_classes", 1)

    # ToDO: to check if we can have multiple models for the same technology, e.g. different turbine models for onshore and offshore wind, or different panel models for rooftop and utility-scale solar. 
    tech = next(t for t in ["panel", "turbine"] if t in params["resource"])
    model_names = [params["resource"][tech]]


    # Get the default cutout name
    default_cutout = snakemake.config["atlite"]["default_cutout"]
    # Get the time range for that cutout
    cutout_time = snakemake.config["atlite"]["cutouts"][default_cutout]["time"]
    # Extract the year from the start date
    year = int(cutout_time[0][:4])

    for model in model_names:
        config_path = Path(
                pkg_resources.files("corres.config_files") / "config_api.yml"
            )
        if not config_path.is_file():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with config_path.open() as f:
            config_corres= yaml.load(f, Loader=yaml.FullLoader)  
        
        config_corres["corres_data_folder"]=snakemake.config.get("corres", {}).get("data_folder", "")
        
        config_corres["years"] = [year]

        config_corres["output"] =str(Path(snakemake.output.profile_corres).parent)


        config_corres["shapefile_fn"]=snakemake.input.resource_regions
        if technology == "offwind-ac":
            config_corres["shapefile_onoff"]="Offshore_bottom_fixed"
        elif technology == "onwind":
            config_corres["shapefile_onoff"]="Onshore"
        elif technology == "offwind-float":
            config_corres["shapefile_onoff"]="Offshore_floating"

        #resource grades for corres, we use the same bins as for the profiles, but with different names
        suffixes = [str(i) for i in range(nbins - 1, -1, -1)]
        band_names = [f"RG{s}" for s in reversed(suffixes)]
        bands = np.linspace(0.0, 1.0, nbins + 1)
        bands = np.floor(bands * 100) / 100
        bands[-1] = 1.0
        bands = bands.tolist()
        config_corres["resource_grades_band"]["band_names"] = band_names
        config_corres["resource_grades_band"]["bands"] = bands
        

        #Turbine specifications
        if model=="Vestas_V112_3MW":
            config_corres["turbines_specific_power"][config_corres["shapefile_onoff"]]=[305]
            config_corres["turbines_hub_heights"][config_corres["shapefile_onoff"]]=[84]
            config_corres["site_parameters"][config_corres["shapefile_onoff"]]["installed_capacity_MW"]=30
        
        #Save cofing for Corres
        config_corres_path = Path(snakemake.output.profile_corres).parent / "corres_pypsa.yml"
        config_corres_to_save = dict(config_corres)

        with config_corres_path.open("w") as f:
            yaml.safe_dump(config_corres_to_save, f, sort_keys=False)
        logger.info("Saved Corres config: %s", config_corres_path)

        # Run Corres pipeline
        power_xr=run_pipeline(config_corres_path,pypsa=True)
        power_xr.to_netcdf(snakemake.output.profile_corres)
    logger.info("!!!!!!!!!!!!!!!!! Corres Done !!!!!!!!!!!!!!!!!!!!!!! ")
