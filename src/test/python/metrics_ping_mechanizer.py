# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0/LGPL 2.1
#
# The contents of this file are subject to the Mozilla Public License Version
# 1.1 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
# for the specific language governing rights and limitations under the
# License.
#
# The Original Code is Mozilla Bagheera.
#
# The Initial Developer of the Original Code is
# Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2011
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
# Xavier Stevens <xstevens@mozilla.com>
# Alternatively, the contents of this file may be used under the terms of
# either the GNU General Public License Version 2 or later (the "GPL"), or
# the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
# in which case the provisions of the GPL or the LGPL are applicable instead
# of those above. If you wish to allow use of your version of this file only
# under the terms of either the GPL or the LGPL, and not to allow others to
# use your version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the notice
# and other provisions required by the GPL or the LGPL. If you do not delete
# the provisions above, a recipient may use your version of this file under
# the terms of any one of the MPL, the GPL or the LGPL.
#
# ***** END LICENSE BLOCK *****
import mechanize
import urllib2
import uuid
import random
import time

class Transaction:
    
    def __init__(self):
        self.custom_timers = {}
        self.servers = ["localhost"]
        self.url = "http://%s:8080/submit/metrics"
        self.content_type = "application/json"
        self.post_headers = { "Content-Type": self.content_type }
        self.post_data = read_data("metrics_ping.js")
    
    def read_data(self, filename):
        fin = open(filename, "r")
        data = fin.read()
        fin.close()
        return data
    
    def run(self):
        base_url = self.url % random.choice(self.servers)
        post_url = "%s/%s" % (base_url, str(uuid.uuid4()))
        request = urllib2.Request(post_url, self.post_data, self.post_headers)
        start_timer = time.time()
        response = urllib2.urlopen(request)
        latency = time.time() - start_timer
        #self.custom_timers['response time'] = latency
        timer_key = 'response time %d' % (response.code)
        self.custom_timers[timer_key] = latency
        assert(response.code == 200 or response.code == 201 or response.code == 204), 'Bad HTTP Response: %d' % (response.code)
        
if __name__ == '__main__':
    trans = Transaction()
    trans.run()
    
    for t in trans.custom_timers.iterkeys():
        print '%s: %.5f secs' % (t, trans.custom_timers[t])
