import argparse
import ConfigParser

from sources import Twitter_Source
from sinks import S3_Sink
from sinks import Local_Sink

def main():
  parser = argparse.ArgumentParser(
    description = "Data Ingest Social Media Monitoring")
  parser.add_argument(
    '--config', help = "path to configuration file", required = True)
  args = parser.parse_args()

  config = ConfigParser.ConfigParser()
  config.read(args.config)

  ingest = DataIngest(config)
  
  # Initialize sinks
  ingest.init_sink()
  # Initialize source 
  ingest.init_source()

  # If one source and one sink is configured, start ingest
  if ingest.sources and ingest.sinks:
    ingest.start_datacollection()
  else:
    print "Check config settings and retry"

class DataIngest:
  """Flexible source and destination Data Ingest for Social Media"""
  def __init__(self, config):
    self.config = config
    self.sources = [ ]
    self.sinks = [ ]
    
  # ------------------------------------------------------------------
  def init_source(self):
    if 'Twitter' in self.config.sections():
      print 'Creating Twitter source (found [Twitter] section)'
      twitter_config = dict(self.config.items('Twitter'))
      twitter_source = Twitter_Source.Twitter_Source(twitter_config)
      self.sources.append( twitter_source )

    else:
      print "No Source found in config file... Stopping Data Ingest"
      print "Configure Twitter Source in configuration file"

  # ------------------------------------------------------------------
  def init_sink(self):

    if 'S3' in self.config.sections():
      print 'Creating S3 sink (found [S3] section)'
      s3_config = dict(self.config.items('S3'))
      s3_sink = S3_Sink.S3_Sink(s3_config)
      self.sinks.append(s3_sink)

    elif 'Local' in self.config.sections():
      print 'Creating Local sink (found [Local] section)'
      local_config = dict(self.config.items('Local'))
      local_sink = Local_Sink.Local_Sink(local_config)
      self.sinks.append(local_sink)

    else:
      print "No Sink found in config file... Stopping Data Ingest"
      print "Configure a S3 or Local sink in configuration file"
      
  # ------------------------------------------------------------------
  def start_datacollection(self):
    self.sinks[0].write(self.sources[0])

if __name__ == "__main__":
  main()

