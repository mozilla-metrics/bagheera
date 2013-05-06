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
import mechanize
import urllib2
import random
import time

class Transaction(object):
    
    def __init__(self):
        self.custom_timers = {}
        self.servers = ["localhost"]
        self.url = "http://%s:8080/submit/telemetry"
        self.post_headers = { "Content-Type": "application/json", "Content-Encoding": "gzip" }
        self.post_data = self.read_data("telemetry.js.gz")
    
    def read_data(self, filename):
        fin = open(filename, "rb")
        data = fin.read()
        fin.close()
        return data
    
    def run(self):
        post_url = self.url % random.choice(self.servers)
        request = urllib2.Request(post_url, self.post_data, self.post_headers)
        start_timer = time.time()
        response = urllib2.urlopen(request)
        latency = time.time() - start_timer
        #self.custom_timers['response time'] = latency
        timer_key = 'response time %d' % (response.code)
        self.custom_timers[timer_key] = latency
        assert(response.code == 201), 'Bad HTTP Response: %d' % (response.code)
        
if __name__ == '__main__':
    trans = Transaction()
    trans.run()
    
    for t in trans.custom_timers.iterkeys():
        print '%s: %.5f secs' % (t, trans.custom_timers[t])
