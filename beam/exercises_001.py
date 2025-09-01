#!/usr/bin/env python
# coding: utf-8

# In[1]:


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import sys
import logging
from pathlib import Path


# In[2]:


# Create and configure logger
log_path = Path("logs/beam_logs.log").resolve()
log_path.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler(sys.stdout)
    ],
)

# Creating an object
logger = logging.getLogger()

# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)
logging.info("==========")
logging.info("starting a new notebook execution")

# setting debug variables
default_input_file = Path("../data/sample.txt").resolve()
default_output_dir = Path("../output/").resolve()


# # Pipeline options
# 
# Arguments passed from the terminal at the time of execution.
# 
# Terminal
# ```sh
# --option=value
# ```
# Python:
# ```python
# beam_options = PipelineOptions()
# ```
# 
# Some examples:
# - input
# - output

# In[3]:


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input',
                            default=str(default_input_file),
                           help='File path to be processed as input.')
        parser.add_argument('--output',
                            default=str(default_output_dir),
                           help='Path prefix for the output file(s).')
        parser.add_argument('--platform',
                            default='notebook',
                            help='notebook= executed as a notebook, script=executed from terminal')


# In[4]:


def main(argv=None):
    # setting up the pipeline options
    argv = argv or sys.argv[1:] 
    options = PipelineOptions(argv).view_as(MyOptions)
    logging.debug(f"Pipeline options initialized as: input={options.input}")
    logging.debug(f"output={options.output} - Platform:{options.platform}")

    with beam.Pipeline(options=options) as pipeline:
        pass


# In[5]:


if __name__ == '__main__':
    main()


# In[ ]:




