This repository consists of two example programs for working with the NLA
programatically:

1.  nla_request_date_range
    This will request that some Sentinel 3A data between two dates is restored
    to the archive from the NLA.
    When the request is made, a file is created in $HOME/nla_requests with the
    filename $req_id_nla_request.json, where $req_id is the request id on the
    NLA.
    The $req_id_nla_request.json file contains a JSON dictionary containing the
    following:
        req_id : <request id on the NLA server>
        start_date : <ISO formatted start date of the request>
        end_date : <ISO formatted end date of the request>
        label : <label on the NLA for human readability>

    This file is required by the second program, which does some monitoring.

2.  nla_monitor_requests
    This scans the $HOME/nla_requests directory for _nla_request.json files.
    For those it finds it:
        1. Gets a list of files in the request (from the NLA server)
        2. Calculates the common path of these
        3. Gets the list of files with a RESTORED state in the common path
        4. Compares the list of files in 1. with those in 3.
        5. If all the files in 1. are also in 3. then the request has finished
        6. Completed requests are appended to a list so that all completed
        requests can be dealt with at once.

    Also in this file is the correct way to expire a request (expire_request)
    function, which also deleted the $req_id_nla_request.json file
