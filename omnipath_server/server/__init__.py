#!/usr/bin/env python

#
# This file is part of the `omnipath_server` Python module
#
# Copyright 2024
# Heidelberg University Hospital
#
# File author(s): OmniPath Team (omnipathdb@gmail.com)
#
# Distributed under the GPLv3 license
# See the file `LICENSE` or read a copy at
# https://www.gnu.org/licenses/gpl-3.0.txt
#

"""
Multiple service Twisted Matrix web server.
"""


from twisted.web import server, resource


class Server(resource.Resource):
    isLeaf = False

    def __init__(self):
        """
        Main server managing all services.
        """

        super().__init__()
        self.services = {}


    def render(self, req):
        """
        Dispatch request to the appropriate server.
        """

        if (host := req.getHeader('host')) in self.services:

            return self.services[host].render(req)

        req.setResponseCode(500)

        return b'No such service.'
