import sys
import io
import time
import uuid
import AdvertisersRepository
from flask import Flask, send_file, request, abort
from AvroAppender import AvroAppender

# This is not the best way to do it I guess,few things that should be done is
# avroappender replacement, either by number of records or on daily basis,
# also need scheduler for automatic refresh of active advertisers


app = Flask(__name__)
appender = AvroAppender('avro.log')
mongoRepository = AdvertisersRepository.Repository()

PIXEL_IMAGE = open("Cleargif.gif", "rb").read()


@app.route("/")
def pixel():
    advertiser_id = request.args.get("advertiser")
    if advertiser_id not in mongoRepository.get_active_advertisers():
        return abort(403)

    byte_io = io.BytesIO(PIXEL_IMAGE)
    response = send_file(byte_io, mimetype="image/gif")

    user = request.cookies.get("user")
    if user is None:
        user = str(uuid.uuid4())
        response.set_cookie("user", user)

    appender.log_append(user=user, advertiser=advertiser_id, ip=request.remote_addr, agent=request.headers.get('User-Agent'), time=time.time(), keywords=request.args.get("st"))
    return response


@app.errorhandler(403)
def advertiser_not_found(e):
    return "missing advertiser= or invalid advertiser id"


@app.route("/reload")
def reload_advertisers():
    mongoRepository.reload_active_advertisers()
    return "reloaded active advertisers"

#this is not the best way to handle it,
@app.route("/bye")
def shutdown():
    appender.close_appender()
    mongoRepository.close_connection()
    print "good bye"
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()



if __name__ == "__main__":
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = 8080
    print 'active advertisers ' + mongoRepository.get_active_advertisers()
    app.run(host='0.0.0.0', port=port)