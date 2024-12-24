"""
Multiple service Twisted Matrix web server.
"""


from twisted.web import server, resource


class Server(resource.Resource):
    isLeaf = False

    def __init__(self):

        super().__init__()
        self.services = {}


    def render(self, req):

        if (host := req.getHeader('host')) in self.services:

            return self.services[host].render(req)

        req.setResponseCode(500)

        return b'No such service.'
