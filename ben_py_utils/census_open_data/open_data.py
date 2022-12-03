

@Cache_wrapper(logger=logger,  path_cache=join(DEFAULT_DATA_DOWNLOAD_PATH,"qc_neighborhoods.geojson"))
def download_qc_city_neighborhoods():
    url_qc_city = "https://www.donneesquebec.ca/recherche/dataset/5b1ae6f2-6719-46df-bd2f-e57a7034c917/resource/436c85aa-88d9-4e57-9095-b72b776a71a0/download/vdq-quartier.geojson"
    shp_qc_city = gpd.read_file(url_qc_city).to_crs(4326)        

    return shp_qc_city
