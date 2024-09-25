from .pipeline import Pipeline


class IMERGPipeline(Pipeline):
    def __init__(self, mode, is_update, start_year, end_year, log_level, **kwargs):
        super().__init__(
            container_name=kwargs["container_name"],
            raw_path=kwargs["raw_path"],
            processed_path=kwargs["processed_path"],
            log_level=log_level,
            mode=mode,
            metadata=kwargs["metadata"],
            use_cache=kwargs["use_cache"],
        )
        self.is_update = is_update
        self.start_year = start_year
        self.end_year = end_year

    def _generate_raw_filename(self, year):
        # TODO
        pass

    def _generate_processed_filename(self, date):
        # TODO
        pass

    def query_api(self, year):
        filename = self._generate_raw_filename(year)
        # TODO
        return filename

    def process_data(self, raw_filename):
        # TODO
        pass

    def run_pipeline(self):
        self.logger.info(f"Running IMERG pipeline in {self.mode} mode...")
        if self.is_update:
            self.logger.info("Retrieving IMERG data from yesterday...")
            # TODO
            pass
        else:
            self.logger.info(
                f"Retrieving IMERG data from {self.start_year} to {self.end_year}..."
            )
            for year in range(self.start_year, self.end_year + 1):
                # TODO
                pass
        self.logger.info("Completed IMERG update.")
