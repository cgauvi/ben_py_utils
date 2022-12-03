
def download_zip_shp(url :str,
                     data_download_path  :str = DEFAULT_DATA_DOWNLOAD_PATH ) -> gpd.GeoDataFrame :
    """
    download_zip_shp Download a zipped shp file from a url + save results

    Only meant to work with zipped shp files, for geojson just read in using .read_file()

    Args:
        url (str): url 
        data_download_path (str, optional): path to save the zipped file. Defaults to DEFAULT_DATA_DOWNLOAD_PATH.

    Returns:
        gpd.GeoDataFrame: geopandas df
    """

    # e.g. extract ghy_000c11g_e from the following url
    # 'https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/ghy_000c11g_e.zip'
    file_download = Path(url).stem

    ## Set the download path 
    path_data_dir_unzipped = join(data_download_path, file_download)  # path after unzipping 
    path_data_dir_zip = path_data_dir_unzipped + ".zip" # path of zipped file e.g. ../postal_code_voronoi/data/lpr_000a21a_e.zip
    path_data_dir_unzipped_shp = join(path_data_dir_unzipped, Path(file_download).stem + ".shp") # path of actual shp file e.g. ../postal_code_voronoi/data/lpr_000a21a_e/lpr_000a21a_e.shp


    ## Download and unzip as required 
    if not isfile(path_data_dir_unzipped_shp):
        response= requests.get(url)
        makedirs(dirname(path_data_dir_unzipped),exist_ok=True)
        with open(path_data_dir_zip, "wb", encoding='utf8', errors='ignore') as f:
            f.write(response.content)

        with zipfile.ZipFile(path_data_dir_zip, 'r', encoding='utf8' ) as zip_ref:
            zip_ref.extractall(path_data_dir_unzipped)

        remove(path_data_dir_zip)

        shp = gpd.read_file(path_data_dir_unzipped_shp)
    else:
        shp = gpd.read_file(path_data_dir_unzipped_shp)

    return shp
