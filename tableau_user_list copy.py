import csv
import configparser
import datetime
import pytz
# import requests
# import logging
# import urllib3
import tableauserverclient as TSC
from tableauserverclient import RequestOptions

# requests.packages.urllib3.disable_warnings()
# urllib3_logger = logging.getLogger("urllib3")
# urllib3_logger.setLevel(logging.DEBUG)

# load configuration from the config.ini file
def load_config():
    config = configparser.ConfigParser()

    # Read configuration from config.ini file
    config.read('config.ini')
    return config

# fetch all users from Tableau Server
def fetch_all_users(server):
    # Defining initial page size and create list to hold users
    page_size = 1000
    all_users = []

    # Create RequestOptions object for pagination
    req_options = RequestOptions()
    req_options.pagesize = page_size

    # Fetch the first page of users and extend the user list
    users, pagination_item = server.users.get(req_options)
    all_users.extend(users)

    # Loop to fetch more users while there are more available
    while pagination_item.page_number * req_options.pagesize < pagination_item.total_available:
        req_options.pagenumber = pagination_item.page_number + 1
        more_users, pagination_item = server.users.get(req_options)
        all_users.extend(more_users)
    return all_users

# filter the list of users based on the last login date and user role
def filter_users(users, cutoff_date, excluded_roles):
    # Empty list to hold filtered users
    filtered_users = []

    # Loop over all users
    for user in users:
        # Exclude users with roles that are in the excluded_roles list
        if user.site_role not in excluded_roles: 
            # Include users who have never logged in or last logged in before the cutoff date
            if user.last_login is None or user.last_login < cutoff_date:
                filtered_users.append(user)
    return filtered_users

# write user data to a CSV file
def write_users_to_csv(users, filename):
    with open(filename, 'w') as f:
        writer = csv.writer(f)

        # Write the header row
        writer.writerow(['Name', 'Email', 'Role', 'Last Login'])

        # Loop over all users and write their data
        for user in users:
            last_login_str = user.last_login.strftime("%Y-%m-%dT%H:%M:%S%Z") if user.last_login else None
            writer.writerow([user.name, user.email, user.site_role, last_login_str])

def main():
    config = load_config()

    # Create the TableauAuth object and Server object
    tableau_auth = TSC.TableauAuth(config['Tableau']['username'], config['Tableau']['password'], site_id=config['Tableau']['site_id'])
    server = TSC.Server(config['Tableau']['server_url'], use_server_version=True)
    server.add_http_options({'verify': False})  # Disable SSL verification

    with server.auth.sign_in(tableau_auth):
        # Fetch all users
        users = fetch_all_users(server)

        # Parse cutoff date from config and make it timezone aware
        cutoff_date = datetime.datetime.strptime(config['Filter']['cutoff_date'], "%Y-%m-%d")
        cutoff_date = cutoff_date.replace(tzinfo=pytz.UTC)

        # Get the list of roles to exclude from config
        excluded_roles = config['Filter']['excluded_roles'].split(", ")

        # Filter the users
        filtered_users = filter_users(users, cutoff_date, excluded_roles)

        # Write filtered users to CSV file
        write_users_to_csv(filtered_users, config['Tableau']['csv_filename'])

    print(f"The user list has been saved to '{config['Tableau']['csv_filename']}'")

if __name__ == "__main__":
    main()
