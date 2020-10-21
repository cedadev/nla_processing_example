import argparse
import os
import pathlib
from datetime import date, timedelta
import json

from nla_client import nla_client_lib as nla

"""
Purpose: Example script for using the nla_client_lib to make requests to the NLA
programmatically.  Successfull requests are stored in JSON files in the
"nla_requests" directory in the user's home directory

Author:  Neil Massey
Date:    21/10/2020
"""

# custom exception for correct error handling
class NLAException(Exception):
    pass

PATH_STUB = "/neodc/sentinel3a/data/SLSTR/L1_RBT"
MONITOR_PATH = "{}/nla_requests".format(os.environ["HOME"])

def get_nla_file_list(path_stub, start_date, end_date):
    # iterate over the dates and build the list of files, using nla.ls to
    # find the files

    # maintain a list of files to retrieve
    files_to_retrieve = []

    for d in range((end_date-start_date).days+1):
        date_to_find = start_date+timedelta(d)
        file_to_find = ("{}/{}/{}/{}".format(
                            path_stub, date_to_find.year,
                            date_to_find.month,
                            date_to_find.day
                        ))
        # do an ls on the NLA server - stages = "T" indicates only to return
        # those on tape
        file_found = nla.ls(file_to_find, stages="T")
        # file_list is a nested dictionary:
        # {count: <number of returned files>,
        #  files: [{path: <file path>,
        #           stage: <UDTAR>,
        #           verified: <datetime of verified date>
        #           size: <integer> size of file in bytes}
        #         ]
        # }
        for e in range(file_found["count"]):
            # get the filename using the above info
            filename = file_found["files"][e]["path"]
            # append to files_to_retrieve list
            files_to_retrieve.append(filename)
    return files_to_retrieve

def make_nla_request(file_list, start_date, end_date, label):
    # make a request to the NLA and get the request id back
    # save the request id in a file, along with the start date, end date and
    # label

    # make sure the directory where we save our monitoring files exists
    monitor_path = pathlib.Path(MONITOR_PATH)
    if not monitor_path.is_dir():
        monitor_path.mkdir(parents=True, exist_ok=True)

    response = nla.make_request(files=file_list, label=label)

    # this returns a response object.  If the status is 200 (OKAY) then we
    # can decode the data from JSON to a dictionary
    if response.status_code == 200:
        response_details = response.json()
        request_id = response_details["req_id"]
        # write out the request details to a file - JSON format, file name is
        # request id
        monitor_file_name = "{}_nla_request.json".format(request_id)
        # create the dictionary
        out_dict = {"req_id" : request_id,
                    "start_date" : start_date.isoformat(),
                    "end_date" : end_date.isoformat(),
                    "label" : label}
        # open the file, write the json dump as a string
        with open(monitor_path / monitor_file_name, 'w') as fh:
            fh.write(json.dumps(out_dict))

    else:
        raise NLAException("Could not make a request with label {}".format(
                            label))

if __name__ == "__main__":
    # get the start and end date from the command line
    # create the argument parser
    parser = argparse.ArgumentParser(
        prog="nla_process_example",
        formatter_class=argparse.RawTextHelpFormatter,
        description=(
            "Demonstration of programmatically interacting with the NLA."
        )
    )

    # add the arguments
    parser.add_argument('-s', '--start_date', action="store", default="",
        metavar="<start_date>", required=True,
        help=(
            "Start date of the data to be retrieved.  In ISO format: "
            "yyyy-mm-dd"
        )
    )

    parser.add_argument('-e', '--end_date', action="store", default="",
        metavar="<end_date>", required=True,
        help=(
            "End date of the data to be retrieved.  In ISO format: "
            "yyyy-mm-dd"
        )
    )

    # parse the arguments
    args = parser.parse_args()

    # process the arguments
    if args.start_date:
        start_date = date.fromisoformat(args.start_date)

    if args.end_date:
        end_date = date.fromisoformat(args.end_date)

    # from the start date and end date get a list of files from the NLA
    file_list = get_nla_file_list(PATH_STUB, start_date, end_date)
    if len(file_list) == 0:
        raise NLAException("No files found ONTAPE for specified date range")
    # create a label for the request
    label = "{} : {} to {} ".format(PATH_STUB, start_date, end_date)
    # make a request to the NLA to fetch those files and get the request id back
    req_id = make_nla_request(file_list, start_date, end_date, label)
