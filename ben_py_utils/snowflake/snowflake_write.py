from abc import ABC
import pandas as pd
import geopandas as gpd
import re
from os.path import exists
from pathlib import Path
from typing import Union

from snowflake.connector.connection import SnowflakeConnection

class Abstract_Sfkl_writer(ABC):

    def __init__(self,
                conn: SnowflakeConnection,
                sfkl_stage_name: str,
                local_file_path_to_upload :Union[str, Path]):

        self.con = con
        self.sfkl_stage_name = sfkl_stage_name
        self.local_file_path_to_upload = local_file_path_to_upload


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):

        # Clean up and remove the stage at the end if it exists
        if self.sfkl_stage_name is not None:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP STAGE IF EXISTS {self.sfkl_stage_name}")
            except Exception as e:
                pass


    def _create_staging(self):
        
        with conn.cursor() as cursor:

            # Internal staging area (in snowflake)
            cursor.execute(f"create or replace stage {self.sfkl_stage_name}")

            # Upload csv to snowflake staging area
            assert exists(self.local_file_path_to_upload), \
                    f"Fatal error when trying to create stage!" \
                    f"Local file {self.local_file_path_to_upload} does not exist"

            cursor.execute(f"put file://{self.local_file_path_to_upload} @{self.sfkl_stage_name};")

            # Create the final table if it does not exist
            ## Determine if the table exists (can have 0 rows), but MUST exist
            db_exists <- check_db_exists(con, sfkl_table_name)
            assertthat::assert_that(db_exists,
                                    msg = glue::glue("Fatal error!
                                                    table {sfkl_table_name} does not exist"
                                                    )
                                    )



    def write_to_sklf(seflt):
        raise NotImplementedError("")



class Gpd_Sfkl_writer(Abstract_Sfkl_writer):

    def __init__(self,
                conn: SnowflakeConnection):

        super.__init__(conn) 

    

    def gpd_to_sfkl(conn:SnowflakeConnection, ):



  ## Insert into from the table from the staging file
  num_cols <- read.table(local_file_path, sep = field_delimiter) %>% ncol
  # Try to unquote - undo the effect of the gdal csv driver that always quotes to avoi parsing errors
  select_cols_str <- paste0('REPLACE(t.$',
                            seq(from = 2, to = num_cols, by =1),
                            ',\'\"\',',
                            '\'\')',
                            collapse = ',')

  # Create the file format
  dbExecute(con,
            glue("create or replace file format myformat type =  '{file_format}' field_delimiter = '{field_delimiter}' skip_header = 1 NULL_IF = ({list_na_strings});")
  )

  # Copy from the staging area to the table
  # Logic depends on whether we have a geometry column or not
  # Not great, but man this was painful
  # TODO: make the formatting of the stage use the same code for both control flows
  if(is_spatial){

    dbExecute(con,
              glue("insert into {sfkl_table_name}
                 select {select_cols_str}, TO_GEOMETRY(REPLACE(t.$1, '\"', ''))
                 from @{sfkl_stage_name} (file_format => 'myformat') as t;")
    )
  }
  else{

    first_str <- paste0('REPLACE(t.$1', ',\'\"\',' ,  '\'\')')
    select_cols_str <- paste0(first_str, ',', select_cols_str )

    dbExecute(con,
              glue("insert into {sfkl_table_name}
                 select {select_cols_str}
                 from @{sfkl_stage_name} (file_format => 'myformat') as t;")
    )
  }