
from ben_py_utils.misc.
from ben_py_utils.misc.cache import Cache_wrapper
from ben_py_utils.misc.constants import DATA_DIR

# Cartographic is very precise (hence very heavy) whereas digital is coarser and faster to retrieve and process
USE_CARTOGRAPHIC = False

@Cache_wrapper(path_cache=join(DATA_DIR, "canada_water.geojson"))
def download_water():
    ## Download lakes and riers
    url_water_lakes_rivers="https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/ghy_000c11a_e.zip"
    shp_rivers = download_zip_shp(url_water_lakes_rivers) 
    shp_rivers = shp_rivers.to_crs(4326)

    return shp_rivers
    


@Cache_wrapper(path_cache=join(DATA_DIR,"qc_das.geojson"))
def download_qc_das(use_cartographic:bool = USE_CARTOGRAPHIC,
                    data_download_path  :str = DATA_DIR )  -> gpd.GeoDataFrame:
    """
    download_qc_das Read in the 2021 dissemination areas (DAs) for Province of Quebec

    Args:
        use_cartographic (bool): _description_

    Returns:
        gpd.GeoDataFrame: _description_
    """

    logger.info(f"Saving data to {data_download_path}")

    zip_download_url = "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lda_000b21a_e.zip" \
    if use_cartographic \
    else "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lda_000a21a_e.zip"

    shp_das_all = download_zip_shp(zip_download_url,data_download_path).\
        to_crs(4326)

    shp_qc = download_qc_boundary(use_cartographic,data_download_path).\
        to_crs(4326)

 
    num_qc_das= np.sum(shp_das_all.DAUID.str[:2] == "24")

    shp_das_qc = gpd.sjoin(shp_das_all, shp_qc, how="inner", op="within" )

    assert num_qc_das == shp_das_qc.shape[0], f"Error, there are {shp_das_qc.shape[0]} DAs found by spatial join but {num_qc_das} based on DAUID filtering"

    print(f"There are {shp_das_qc.shape[0]} features/das in Quebec for the 2021 census")

    return shp_das_qc

@Cache_wrapper(path_cache=join(DATA_DIR,"qc_fsa_2016.geojson"))
def download_qc_fsas_2016(use_cartographic:bool = USE_CARTOGRAPHIC,
                          data_download_path  :str = DATA_DIR ) -> gpd.GeoDataFrame:
    """
    download_qc_boundary Read the 2016 FSAs for the province of Quebec

    Args:
        use_cartographic (bool): _description_

    Returns:
        _type_: _description_
    """
    zip_download_url = "https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lfsa000a16a_e.zip" \
    if use_cartographic \
    else "https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lfsa000b16a_e.zip"

    shp_fsa_all = download_zip_shp(zip_download_url,data_download_path).\
        to_crs(4326)

    shp_fsa_all_qc = shp_fsa_all.loc[shp_fsa_all.PRUID.astype('str') =="24", ]
 
    print(f"There are {shp_fsa_all_qc.shape[0]} FSAs in Quebec for the 2016 census")

    return shp_fsa_all_qc




@Cache_wrapper(path_cache=join(DATA_DIR,"qc_city_fsa_2016.geojson"))
def download_qc_city_fsa_2016(use_cartographic:bool = USE_CARTOGRAPHIC,
                              buffer_degrees=0.1,
                              data_download_path  :str = DATA_DIR ) -> gpd.GeoDataFrame:


    # Select only the fsa within the province 
    shp_2016_fsas_qc_province = download_qc_fsas_2016(use_cartographic, data_download_path)

    # Get the Quebec city neighborhood polygons
    shp_qc_city = download_qc_city_neighborhoods()

    # Get intersecting polygons
    idx_intersects = shp_2016_fsas_qc_province.geometry.intersects(shp_qc_city.unary_union.buffer(buffer_degrees) )
    shp_2016_fsas_qc_city = shp_2016_fsas_qc_province[idx_intersects]

    return shp_2016_fsas_qc_city



@Cache_wrapper(path_cache=join(DATA_DIR,"qc_city_fsaldu_2016_df.geojson"))
def download_qc_city_fsaldu_2016_df(**kwargs)-> pd.DataFrame:

    # Select only the fsaldu within the fsa within the city
    shp_2016_fsas_qc_city = download_qc_city_fsa_2016(**kwargs)

    # Get the pccf file
    df_pccf = get_df_pccf()

    # Extract the FSA
    df_pccf['FSA'] = df_pccf.POSTCODE.str[:3]

    # Join to filter by province (PCCF has all Canadian PCs)
    df_pccf_qc_city = pd.merge(df_pccf,
            shp_2016_fsas_qc_city[['CFSAUID']]   ,
            left_on = 'FSA' ,
            right_on =  'CFSAUID')

    return df_pccf_qc_city



@Cache_wrapper(path_cache=join(DATA_DIR,"qc_province.geojson"))
def download_qc_boundary(use_cartographic:bool = USE_CARTOGRAPHIC,
                        data_download_path  :str = DATA_DIR ) -> gpd.GeoDataFrame:
    """
    download_qc_boundary Read the 2021 Province of Quebec boundary files 

    Args:
        use_cartographic (bool): _description_

    Returns:
        _type_: _description_
    """

    zip_download_url = "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lpr_000b21a_e.zip" \
    if use_cartographic \
    else "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lpr_000a21a_e.zip"

    shp_prov = download_zip_shp(zip_download_url)

    ## Get Quebec
    shp_qc = shp_prov[ shp_prov.PRUID.astype('str') == "24"]
    shp_qc = shp_qc.to_crs(4326)
    assert shp_qc.shape[0] ==1

    return shp_qc


