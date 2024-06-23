from dagster import ConfigurableIOManager, OutputContext
import pandas as pd
import os

class CsvIOManager(ConfigurableIOManager):

    path_prefix: str = ""

    def _get_path(self, context) -> str:
        os.makedirs(self.path_prefix, exist_ok=True)   # make sure the directory exists
        return self.path_prefix + "/".join(context.asset_key.path) + ".csv"

    def handle_output(self, context: OutputContext, df: pd.DataFrame):
        current_path = self._get_path(context)
        if os.path.exists(current_path):
            # If the file exists, append the data to the file and keep existing header
            df.to_csv(current_path, mode='a', index=False, header=False, encoding='utf-8-sig')
        else:
            # If the file doesn't exist, create it and write the data with its header
            df.to_csv(current_path, index=False, header=True, encoding='utf-8-sig')