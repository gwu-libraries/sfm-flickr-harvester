from __future__ import absolute_import
import warc as ia_warc
import sys
import StringIO


def wrap_execute(exec_func, debuggable):
    """
    Enables debugging on debuggable, calls an API function, and captures output.
    :param exec_func: the API function.
    :param debuggable: the http client that debug is to be set on.
    :return: results of the function, captured stdout as a StringIO.
    """

    #When debuglevel is set httplib outputs details to stdout.
    #This captures stdout.
    capture_out = StringIO.StringIO()
    sys.stdout = capture_out
    #sys.stdout = Tee([capture_out, sys.__stdout__])
    debuggable.debuglevel = 1
    try:
        return_values = exec_func()
    finally:
        #Stop capturing stdout
        sys.stdout = sys.__stdout__
        debuggable.debuglevel = 0
    return return_values, capture_out


def parse_capture(capture_out):
    """
    Transform the captured stdout into a series of request and response headers
    """

    http_headers = []
    #Reset to the beginning of capture_out
    capture_out.seek(0)
    response_header = None
    for line in capture_out:
        if line.startswith("send:"):
            #Push last req and resp
            if response_header:
                #Response record
                http_headers.append(response_header)
                response_header = None
            start = line.find("GET")
            if start == -1:
                start = line.find("POST")
            assert start != -1
            request_header = line[start:-2].replace("\\r\\n", "\r\n")
            #Request record
            http_headers.append(request_header)
        elif line.startswith("reply:"):
            #Start of the response header
            response_header = line[8:-6] + "\r\n"
        elif line.startswith("header:"):
            #Append additional headers to response header
            response_header += line[8:-2] + "\r\n"
    #Push the last response
    http_headers.append(response_header)

    return http_headers


def parse_url(http_header):
    """
    Parse the url from the http request header.

    Note that this excludes the protocol, host, and port.
    """
    if http_header.startswith("GET"):
        start_pos = 4
    elif http_header.startswith("POST"):
        start_pos = 5
    else:
        assert False, "http header does not start with GET or POST"
    end_pos = http_header.find(" HTTP/")
    assert end_pos != -1
    return http_header[start_pos:end_pos]


def to_warc_record(warc_type, url, http_header=None, http_body=None, concurrent_to_warc_record=None,
                   headers=None):
    warc_headers = {
        "WARC-Target-URI": url,
        "WARC-Type": warc_type
    }
    if headers:
        warc_headers.update(headers)
    if concurrent_to_warc_record:
        warc_headers["WARC-Concurrent-To"] = concurrent_to_warc_record.header.record_id
    payload = None
    if http_header:
        payload = http_header
    if http_body:
            if payload:
                payload += "\r\n" + http_body
            else:
                payload = http_body

    return ia_warc.WARCRecord(payload=payload, headers=warc_headers)


def wrap_api_call(exec_func, debuggable, base_url):
    #Execute the method and capture the output
    raw_resp, capture_out = wrap_execute(exec_func, debuggable)

    #Parse the captured output
    http_headers = parse_capture(capture_out)
    assert len(http_headers) == 2

    #Reconstruct the url
    url = base_url + parse_url(http_headers[0])

    #Create warc records
    request_record = to_warc_record("request", url, http_header=http_headers[0])
    #Create response record
    response_record = to_warc_record("response", url, http_body=raw_resp, http_header=http_headers[1])

    return request_record, response_record, raw_resp
