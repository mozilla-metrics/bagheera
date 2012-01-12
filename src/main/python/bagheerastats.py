#!/usr/bin/env python

"""
Copyright 2012 Xavier Stevens <xstevens@mozilla.com>

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain
a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
"""

import sys
import getopt
import logging
import urllib2

from datetime import datetime, timedelta

from gmetric import GmetricConfig, Gmetric

# Setup logging facilities
log = logging.getLogger("")
log.setLevel(logging.DEBUG)

'''
A class to emit Ganglia metrics based on the stats from Bagheera
'''
class BagheeraStatsMetric:
    def __init__(self, gmetric):
        self.gmetric = gmetric
        
    def emit_row(self, server_urls, gmetric_config):
        try:
            for server_url in server_urls:
                request = urllib2.Request(server_url)
                f = urllib2.urlopen(request)
                cur_name = None
                for line in f:
                    prop = line.split("=")
                    if len(prop) == 2:
                        if prop[0] == "name":
                            cur_name = prop[1].strip()
                        elif cur_name != None:
                            metric_name = "%s.%s" % (cur_name, prop[0].strip())
                            count = int(prop[1].strip())
                            self.gmetric.send_meta(metric_name, gmetric_config.type, gmetric_config.units, gmetric_config.slope, gmetric_config.tmax, gmetric_config.dmax, gmetric_config.group_name)
                            self.gmetric.send(metric_name, count, gmetric_config.type)
                # reset the stats
                reset_request = urllib2.Request(server_url + "/reset")
                f = urllib2.urlopen(reset_request)
                f.read()
        except urllib2.HTTPError, e:
            print "HTTP Error:", e.code , server_url
        except urllib2.URLError, e:
            print "URL Error:", e.reason , server_url       
        
'''
Print usage information
'''
def usage():
    print "Usage: %s [--help] [--hbase_thrift server[:port]] [--gmetric server[:port]] table" % (sys.argv[0])
    
def main(argv=None):
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "gmetric=", "group="])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    
    ganglia_host = "localhost"
    ganglia_port = 8649
    ganglia_protocol = "multicast"
    ganglia_metic_group_name = "bagheera"
    # process options
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif o == "--gmetric":
            splits = a.split(":")
            ganglia_host = splits[0]
            if len(splits) == 2:
                ganglia_port = splits[1]
        elif o == "--group":
            ganglia_metric_group_name = a
                
    # process arguments
    table_name = None
    if len(args) > 0:
        table_name = args[0]
    
    gmetric = Gmetric(ganglia_host, ganglia_port, ganglia_protocol)
    bsm = BagheeraStatsMetric(gmetric)
    gmc = GmetricConfig(type="int32", group_name=ganglia_metic_group_name)
    bsm.emit_row(["http://localhost:8080/stats"], gmc)                

    
if __name__ == "__main__":
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(console)
    sys.exit(main())