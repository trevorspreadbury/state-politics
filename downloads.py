import requests
import json
import re
import os
from legislator import Legislator
from constants import state_abbreviations
from bs4 import BeautifulSoup
import zipfile
import yaml


def get_legislators(key, state, body):
    """ Create Legislator instances for all members of given state house

    Args:
        key (string): API key for openstates.org
        state (string): Full name of US State (or DC or PR)
        body (string): Name of governmental body of interest ('lower' or
            'upper' for upper and lower houses of legislatures)
    Returns:
        legislators (list of Legislators): List of Legislator objects
            representing members of selected body.
    """
    url = "https://v3.openstates.org/people"
    legislators = []
    # Set the max page to one to send an initial request. This request
    # will return the true max pages. 
    max_pages = 1
    page = 1
    while page <= max_pages:
        payload = {
            "jurisdiction" : state,
            "apikey" : key,
            "org_classification" : body,
            "page" : page,
            "per_page" : 50,
        }
        r = requests.get(url, params=payload)
        response_dict = json.loads(r.text)
        max_pages = response_dict["pagination"]["max_page"]
        for member in response_dict["results"]:
            legislator = Legislator(json=member)
            legislators.append(legislator)
        page += 1
    return legislators

def get_states_legislators(key, state):
    """ Create Legislator instances for all legislators in a given state

    Args:
        key (string): API key for openstates.org
        state (string): Full name of US State (or DC or PR)
    Returns:
        legislators (list of Legislators): List of Legislator objects
            representing members of state's government
    """
    all_legislators = {}
    for body in ["upper", "lower"]:
        legislators = get_legislators(key, state, body)
        print(len(legislators))
        all_legislators[body] = legislators
    return all_legislators


def get_retired_legislators(state):
    """ Gets retired legislators from openstates github (api is only current)

    Args:
        state (str): Full name of US State (or DC or PR)
    """
    state_abbreviation = state_abbreviations[state].lower()
    base_url = "https://raw.githubusercontent.com"
    retired_url = (
        "https://github.com/openstates/people/tree/main/data/{}/retired"
        "".format(state_abbreviation)
    )
    # This page shows the directory containing yml files with info on retired
    # legislators from the given state
    retired_page = requests.get(retired_url)
    retired_soup = BeautifulSoup(retired_page.text, 'lxml')
    retired_legislator_link_tags = retired_soup.find_all("a", string=re.compile(".*" + "yml"))
    retired_legislator_links = (
        [link.get('href') for link in retired_legislator_link_tags]
    )
    for legislator_link in retired_legislator_links:
        # to get the actual file (not just the html of the github page
        # displaying it), got to raw.githubusercontent and remove blob
        # from the url
        full_link = base_url + legislator_link.replace("blob/", "")
        legislator_yml = requests.get(full_link)
        print(yaml.load(legislator_yml.text))
        break
        # legislator_soup = BeautifulSoup(legislator_yml.text, "lxml")
        # print(legislator_soup.find_all("table"))

def convert_yaml_to_csv_row(yaml_dict):
    """ convert the given legislator yaml to a csv row """
    yaml_dict["current_party"] = yaml_dict["party"][0]["name"]
    yaml_dict["current_district"] = yaml_dict["roles"][0]["district"]
    yaml_dict["current_chamber"] = yaml_dict["roles"][0]["type"]



def bulk_state_download(state, login=None, password=None):
    """ Download bulk csv data on bills for given state from Openstates.org

    Params:
        state (string): Full name of state.
        login (string): Login to openstates account. If not supplied, user
            will be prompted.
        password (string): Password to openstates account. If not supplied,
            user will be prompted.
    """
    state_abbreviation = state_abbreviations[state].lower()
    # Prompt user for username and password if they did not supply it
    if not login:
        login = input("Openstates username: ")
    if not password:
        password = input("Openstates password: ")
    # constants for http requests
    login_url = "https://openstates.org/accounts/login/"
    bulk_download_url = "https://openstates.org/data/session-csv/"
    people_url = (
        "https://data.openstates.org"
        "/people/current/{}.csv".format(state_abbreviation)
    )
    payload = {
        'login': login,
        'password': password
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:83.0) Gecko/20100101"
            " Firefox/83.0"
        ),
        "Referer": "https://openstates.org/accounts/login/"
    }
    # Start a session (to stay logged in) and use csrf token
    with requests.Session() as session:
        # GET login page to retrieve csrf token and login
        login_page = session.get(login_url, headers=headers)
        payload['csrfmiddlewaretoken'] = login_page.cookies['csrftoken']
        p = session.post(login_url, data=payload, headers=headers)
        # GET bulk downloads page and extract all links for desired state
        bulk_download_page = session.get(bulk_download_url, headers=headers)
        soup = BeautifulSoup(bulk_download_page.text, 'lxml')
        session_link_tags = soup.find_all("a", string=re.compile(state + ".*"))
        session_links = [link.get('href') for link in session_link_tags]
        session_links.append(people_url)
        # download zip files from links, unzip them, and delete the zips
        chunk_size = 128
        for link in session_links:
            filename = link.split("/")[-1]
            print(filename)
            r = session.get(link, headers=headers, stream=True)   
            with open(filename, "wb") as fd:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    fd.write(chunk)
            # if the filename is the state abbreviation, it is the legislators
            # csv (not a zip) so does not need to be extracted
            if filename.split(".")[0].lower() == state_abbreviation:
                people_name = state_abbreviation.upper() + "_all_people.csv"
                os.rename(
                    filename,
                    os.path.join(state_abbreviation.upper(), people_name)
                )
                continue
            with open(filename, "rb") as fd:
                z = zipfile.ZipFile(fd)
                z.extractall()
            os.remove(filename)

if __name__ == "__main__":
    with open("api-key.txt", "r") as f:
        key = f.read().strip()

    get_retired_legislators("Illinois")