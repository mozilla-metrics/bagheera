"""
Copyright 2011 Xavier Stevens <xstevens@mozilla.com>

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
import urllib2
import time
import uuid
import numpy
from scipy import stats

help_message = '''
    Flags:
        -p <post-file>
        -n <num-requests> (default: 1)
        -T <content-type> (default: text/plain)
        -h|--help prints this message
    
    Example Usage:
        python http-test.py -n 100 -T application/json -p data/small.json http://localhost:5701/hazelcast/rest/maps/metrics_ping
'''

times = []

def timeit(method):

    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        times.append((te-ts)*1000.0)
        
        #print '%r (%r, %r) %2.4f sec' % (method.__name__, args, kw, te-ts)
        
        return result

    return timed
    
class Usage(Exception):
	def __init__(self, msg):
		self.msg = msg


@timeit
def post(url, post_headers, post_data):
    request = urllib2.Request(url, post_data, post_headers)
    code = 200
    try:
        response = urllib2.urlopen(request)
    except URLError, e:
        code = e.code
    return code

def post_requests(url, n, content_type, post_data):
    post_headers = { "Content-Type": content_type }
    ret_codes = []
    for i in range(0,n):
        post_url = "%s/%s" % (url, str(uuid.uuid4()))
        ret_codes.append(post(post_url, post_headers, post_data))
    return ret_codes

def read_file(post_file):
    f = open(post_file, 'r')
    data = f.read()
    f.close()
    return data
    
def print_time_stats(num_requests, ret_codes):
    print "%d requests" % (num_requests)
    print "%d responses" % (len(ret_codes))
    print "Response code distribution:"
    for item in stats.itemfreq(ret_codes):
        print "\t%d => %d" % (item[0], item[1])
    print "Request times (ms):"
    print "\tmin: %.5f" % (min(times))
    print "\tmean: %.5f" % (numpy.mean(times))
    print "\tmedian: %.5f" % (numpy.median(times))
    print "\tmax: %.5f" % (max(times))
    print "\tstddev: %.5f" % (numpy.std(times))
    print "Quantiles of request times (ms):"
    print "\t25%%: %.5f" % (stats.scoreatpercentile(times, 25))
    print "\t50%%: %.5f" % (stats.scoreatpercentile(times, 50))
    print "\t75%%: %.5f" % (stats.scoreatpercentile(times, 75))
    print "\t90%%: %.5f" % (stats.scoreatpercentile(times, 90))
    print "\t95%%: %.5f" % (stats.scoreatpercentile(times, 95))
    print "\t99%%: %.5f" % (stats.scoreatpercentile(times, 99))
	
def main(argv=None):
	if argv is None:
		argv = sys.argv
	try:
		try:
			opts, args = getopt.getopt(argv[1:], "hn:p:T:", ["help"])
		except getopt.error, msg:
			raise Usage(msg)
			
		# option processing
		post_file = None
		num_requests = 1
		content_type = "text/plain"
		for option, value in opts:
		    if option in ("-h", "--help"):
		        raise Usage(help_message)
		    elif option == "-p":
		        post_file = value
		    elif option == "-n":
		        num_requests = int(value)
		    elif option == "-T":
		        content_type = value
		
		url = None
		if len(args) > 0:
		    url = args[0]
		    
		if url == None or post_file == None:
		    raise Usage(help_message)
		    
		data = read_file(post_file)
		ret_codes = post_requests(url, num_requests, content_type, data)
		print_time_stats(num_requests, ret_codes)
	except Usage, err:
		print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
		print >> sys.stderr, "\t for help use --help"
		return 2


if __name__ == "__main__":
	sys.exit(main())
