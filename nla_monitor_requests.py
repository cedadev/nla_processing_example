import pathlib
import json
import sys
import os
import datetime

from nla_client import nla_client_lib as nla
from nla_request_date_range import NLAException
from nla_request_date_range import MONITOR_PATH

"""
Purpose: Example script for using the nla_client_lib to monitor requests to the
NLA programmatically.  These requests have to have been made using the
"nla_request_date_range" program also in this repository.
The "nla_request_date_range" program stores details of the requests in JSON
files in the "nla_requests" directory in the user's home directory
This program relies on those details to monitor the retrieval progress.

Author:  Neil Massey
Date:    21/10/2020
"""

def get_completed_requests():
    # get a list of the completed requests in the $HOME/nla_requests
    p = pathlib.Path(MONITOR_PATH)
    # store a list of completed requests
    completed_requests = []
    # loop over the files in that directory
    for f in p.iterdir():
        # open the file and read the request details into a dictionary
        with open(f, 'r') as fh:
            request_details = json.load(fh)
            # contact the NLA to get the full request details, using the request
            # id stored in request_details
            response = nla.show_request(request_details['req_id'])
            # if None is returned then the id was not found, write to stderr
            # but keep on going
            if response is None:
                sys.stderr.write(
                    "Request id does not exist on NLA server: {}\n".format(
                        request_details['req_id']
                    )
                )
            else:
                # get the files
                file_list = response['files']
                # Note that looping over these files one by one, and making a
                # request to the nla.ls method for each one would be very time
                # consuming.
                # Instead, we're going to ls the common path for the completed
                # files and make sure each file is in this new list
                cpath = os.path.commonpath(file_list)
                ls_response = nla.ls(cpath, stages='R') # stages = R == RESTORED
                # map the dictionary of files:path to a list of files with the
                # common path that have completed
                completed_cpath_files = [x["path"] for x in ls_response["files"]]
                # now loop over the file list from the request and add 1 to a
                # counter for every file that is in the completed_cpath_files
                completed_count = 0
                for f in file_list:
                    if f in completed_cpath_files:
                        completed_count += 1
                # if the completed count matches the length of the original file
                # list then the request has completed
                if (completed_count == len(file_list)):
                    # add the request details filename to the dictonary of info
                    request_details["req_details_fname"] = f
                    # append the dictionary of information
                    completed_requests.append(request_details)
    return completed_requests

def expire_request(request_id):
    # Here's the correct way to expire your request
    now = datetime.datetime.now().isoformat()
    nla.update_request(request_id, retention=now)

    # remove the nla_request file
    p = pathlib.Path(MONITOR_PATH)
    monitor_file_name = "{}_nla_request.json".format(request_id)
    # check the file exists
    if (p / monitor_file_name).exists():
        os.unlink(p / monitor_file_name)


if __name__ == "__main__":
    # No command line arguments for this program as it is going to read in the
    # request ids from the list of files in $HOME/nla_requests

    # get a list of completed requests.
    # Each of the list entries will be a dictionary with:
    # { req_id     : <request id on NLA>,
    #   start_date : <iso format start date for data>
    #   end_date   : <iso format end date for data>
    # }
    completed_requests = get_completed_requests()
    print(completed_requests)

    # with these completed requests you can do a number of things:
    # 1. Send your job off to be processed
    # 2. Delete the request details file on completion of the job
    #    (otherwise your job will run multiples of times!)
    # 3. Expire your request to free up some space
    expire_request(10803)
