import boto
import datetime
import uuid
import json

import os
import errno
import sys

class Local_Sink:
  """Output data to Local filesystem"""

  def __init__(self, config):
    self.config = config

    path = self.config['path']
    now = datetime.datetime.utcnow().isoformat()
    self.folder = path + '/' + now

    try:
        os.makedirs(self.folder)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    print '[Local] Writing to folder ' + self.folder


  def write(self, source):

    self.record_index = 0
    self.file_index = 0

    # Check for option to indent json output in config file
    # default: no indent 
    if 'indent' in self.config:
      self.indent = int(self.config['indent'])
    else:
      self.indent = None
   
    # Check for batch_size in config file
    # default: 50 per batch
    if 'batch_size' in self.config:
      self.batch_size = int(self.config['batch_size'])
    else:
      self.batch_size = 50

    self.batch = [ ]

    for item in source:
      self.record_index = self.record_index + 1
      self.batch.append(item)
      sys.stdout.write('.') # write a record indicator to stdout
      sys.stdout.flush()

      if self.record_index >= self.batch_size:
        self.flush()

    self.flush()

  def flush(self):

    filename = str(self.file_index).zfill(7)

    file_path = self.folder + '/' + filename
    with open(file_path, 'w') as output_file:
      output_file.write(json.dumps(self.batch, indent=self.indent))

    self.file_index = self.file_index + 1

    self.batch = [ ]
    self.record_index = 0

    print('|') # Write a batch separator with a newline to stdout
    

    

    
