from os.path import join, isfile
import geopandas as gpd
import pandas as pd


from ben_py_utils.misc.cache import Cache_wrapper
from ben_py_utils.misc.constants import DEFAULT_CACHE


def test_cache_gdf():
    
    path_cache = join(DEFAULT_CACHE, 'mtl_neigh.parquet')
    @Cache_wrapper(path_cache = path_cache,
                    pd_save_index=False, 
                    force_overwrite=False )

    def download_mtl_open_data(url = 'https://www.donneesquebec.ca/recherche/dataset/5b1ae6f2-6719-46df-bd2f-e57a7034c917/resource/436c85aa-88d9-4e57-9095-b72b776a71a0/download/vdq-quartier.geojson'):
        shp_mtl = gpd.read_file(url)    
        return shp_mtl

    # Downloads the data + caches successfully
    shp_first_time = download_mtl_open_data()
    assert isfile(path_cache)


    # Saved as parquet
    assert isinstance(gpd.read_parquet(path_cache), gpd.GeoDataFrame)

    # Should still work since fun doesnt get called
    shp_second_time = download_mtl_open_data(url="bogus_url")
    assert shp_second_time.shape[0] > 0



def test_cache_df():
    
    path_cache = join(DEFAULT_CACHE, 'mtl_fire_stations.parquet')
    @Cache_wrapper(path_cache = path_cache,
                    pd_save_index=False, 
                    force_overwrite=False )

    def download_mtl_fire_stations(url = "https://data.montreal.ca/dataset/c69e78c6-e454-4bd9-9778-e4b0eaf8105b/resource/5b9c0e1d-3f75-4e98-b53d-6e979c18cc98/download/casernes.csv"):
        df_mtl = pd.read_csv(url)    
        return df_mtl

    # Downloads the data + caches successfully
    df_first_time = download_mtl_fire_stations()
    assert isfile(path_cache)
    assert isinstance(df_first_time, pd.DataFrame)

    # Saved as parquet
    assert isinstance(pd.read_parquet(path_cache), pd.DataFrame)

    # Should still work since fun doesnt get called
    df_second_time = download_mtl_fire_stations(url="bogus_url")
    assert df_second_time.shape[0] > 0
