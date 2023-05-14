#!/usr/bin/env python3
"""
Wrapper for the requests library to retry requests that fail.
"""
import os
import time
import requests

ODMX_DEFAULT_MAX_RETRIES = int(os.getenv("DEFAULT_MAX_RETRIES", "5"))
ODMX_DEFAULT_RETRY_DELAY = int(os.getenv("DEFAULT_RETRY_DELAY", "60"))


# TODO this breaks anything that wants to check for a specific status code
def _json_request_with_retry(
        request_func,
        *args,
        max_retries=ODMX_DEFAULT_MAX_RETRIES,
        retry_delay=ODMX_DEFAULT_RETRY_DELAY,
        **kwargs):
    """
    Send a request to the specified URL, retrying if it fails.

    @param url
    @param max_retries optional, default 5
    @param retry_delay optional, default 60
    @returns response
    """
    for _ in range(max_retries):
        try:
            response = request_func(*args, **kwargs)
            if response.status_code == 200:
                return response
            print(f"Request failed: {response.status_code} {response.text}")
            raise requests.exceptions.ConnectionError("Request failed")
        except requests.exceptions.ConnectionError:
            print(f"Connection error, retrying in {retry_delay} seconds")
            time.sleep(retry_delay)
    raise requests.exceptions.ConnectionError("Max retries exceeded")

# Always save the original requests functions
# so we can write get_orig even if it's not patched
requests.head_orig = requests.head
requests.put_orig = requests.put
requests.patch_orig = requests.patch
requests.delete_orig = requests.delete
requests.get_orig = requests.get
requests.post_orig = requests.post

ODMX_PATCH_REQUESTS = os.getenv("PATCH_REQUESTS", "1") == "1"
if ODMX_PATCH_REQUESTS:
    requests.head = lambda *args, **kwargs: _json_request_with_retry(
        requests.head_orig, *args, **kwargs)
    requests.put = lambda *args, **kwargs: _json_request_with_retry(
        requests.put_orig, *args, **kwargs)
    requests.patch = lambda *args, **kwargs: _json_request_with_retry(
        requests.patch_orig, *args, **kwargs)
    requests.delete = lambda *args, **kwargs: _json_request_with_retry(
        requests.delete_orig, *args, **kwargs)
    requests.get = lambda *args, **kwargs: _json_request_with_retry(
        requests.get_orig, *args, **kwargs)
    requests.post = lambda *args, **kwargs: _json_request_with_retry(
        requests.post_orig, *args, **kwargs)
    print("Notice: Requests patched to retry on failure. "
          "To disable, set env var ODMX_PATCH_REQUESTS=0")
