# -*- coding: utf-8 -*-
import logging
log_file = "job.log"
logging.basicConfig(
                    level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[
                                logging.FileHandler(log_file, mode='w'),
                                logging.StreamHandler()
                             ]
                    )