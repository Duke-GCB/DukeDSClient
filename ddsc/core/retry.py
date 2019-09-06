
class RetrySettings(object):
    # DukeDS API Retry Settings
    # Settings for retrying a ConnectionError connecting to the DukeDS API
    CONNECTION_RETRY_TIMES = 5
    CONNECTION_RETRY_SECONDS = 1
    # Settings for how long to sleep when we receive 503 from DukeDS API (Weekly system maintenance typically)
    SERVICE_DOWN_RETRY_SECONDS = 60
    # How long to sleep when waiting for DukeDS API to be consistent (waits forever)
    RESOURCE_NOT_CONSISTENT_RETRY_SECONDS = 2

    # Backend Store Retry Settings
    # Settings for retrying when a ConnectionError occurs uploading a file chunk
    SEND_EXTERNAL_PUT_RETRY_TIMES = 4
    SEND_EXTERNAL_RETRY_SECONDS = 20
    # Times to retry after receiving a 403 uploading a file chunk (we recreate the URL before retrying)
    SEND_EXTERNAL_FORBIDDEN_RETRY_TIMES = 2
    # Settings for retrying when downloading part of a file
    FETCH_EXTERNAL_PUT_RETRY_TIMES = 5
    FETCH_EXTERNAL_RETRY_SECONDS = 20
