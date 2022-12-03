from os.path import join, isfile
import geopandas as gpd

from ben_py_utils.misc.cache import Cache_wrapper
from ben_py_utils.misc.constants import DEFAULT_CACHE


def test_cache():
    
    path_cache = join(DEFAULT_CACHE, 'mtl_neigh.parquet')
    @Cache_wrapper(path_cache = path_cache,
                    is_geo = False, 
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
    shp_first_time = download_mtl_open_data(url="bogus_url")
    assert shp_first_time.shape[0] > 0
