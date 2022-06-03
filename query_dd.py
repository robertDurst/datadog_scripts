import json
import requests
import time

# Constants
HOUR_INTERVAL = 12
DAYS = 1
START = HOUR_INTERVAL
END = (24 * DAYS) + HOUR_INTERVAL
LIMIT = 5000

DOGWEB_ENV_VAR = ''
_DD_S_ENV_VAR = ''
AUTH_TOKEN = ''

UNIQUE_GRAPHQL_MEMBER_QUERY = '@graphql.operation_name:getUserIdentifiers'
UNIQUE_GRAPHQL_ADMIN_QUERY = '@graphql.operation_name:getFullMemberDetails'
UPDATE_APPOINTMENT_QUERY = '@graphql.operation_name:UpdateAppointment'
RESCHEDULE_APPOINTMENT_QUERY = '@graphql.operation_name:RescheduleAppointment'
CREATE_APPOINTMENT_QUERY = '@graphql.operation_name:CreateAppointment'
CANCEL_APPOINTMENT_QUERY = '@graphql.operation_name:CancelAppointment'

def create_prod_query(query):
    return 'env:production AND (' + query + ')'

def execute_datadog_request(from_hrs, to_hrs, query):
    json_data = {
        'list': {
            'time': {
                'offset': -7200000,
                'from': 'now-' + str(from_hrs) + 'h',
                'to': 'now-' + str(to_hrs) + 'h',
            },
            'indexes': ['*'],
            'search': {
                'query': query
            },
            'columns': [],
            'limit': LIMIT,
            'sort': { 'field': { 'path': 'service', 'order': 'desc' } },
            'computeCount': False,
        },
        '_authentication_token': AUTH_TOKEN,
    }

    cookies = {
        'dogweb': DOGWEB_ENV_VAR,
        '_dd_s': _DD_S_ENV_VAR,
    }

    response = requests.post('https://app.datadoghq.com/api/v1/logs-analytics/list', params={ 'type': 'logs' }, cookies=cookies, headers={}, json=json_data)

    return json.loads(response.content)

def execute_for_iterations_on_query(start, end, query):
    all_events = []

    while start < end:
        from_hrs = start
        to_hrs = start - HOUR_INTERVAL

        events = execute_datadog_request(generic_request(from_hrs, to_hrs, query))['result']['events']
        all_events += events

        print("[Query]", "FROM:", to_hrs, "TO:", from_hrs, "RECORDS:", len(events), '(max ' + str(LIMIT) + ')')
        
        time.sleep(1)
        start += HOUR_INTERVAL

    return events

# an example to get all users by ID
def get_users_by_ip(start, end):
    query = create_prod_query(UNIQUE_GRAPHQL_MEMBER_QUERY)
    events = execute_for_iterations_on_query(start, end, query)
    users = {}

    for event in events:
        e = event['event']
        client_ip = e['custom']['network']['client_ip']
        users[client_ip] = True
   
    return users

get_users_by_ip(START, END)
