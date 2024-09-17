"""
tests used to audit the files in the etl-pipelines repository
"""
# pylint: disable=W1203 # fstrings in logs
# pylint: disable=C0301 # line over 100 chars
# pylint: disable=E0401 # can't find import (due to local import)
# pylint: disable=C0413 # import not at top of doc (due to local import)
# pylint: disable=W0612 # unused variables (due to test reusing functions with 2 outputs)
# pylint: disable=W0621 # redefining from outer scope triggering on pytest fixtures

import sys
import os
from unittest.mock import patch, Mock
import json
import pytest
from dotenv import load_dotenv
from dreams_core import core as dc


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../geckoterminal_coin_metadata')))
import geckoterminal_coin_metadata as gtm # type: ignore[reportMissingImports]

load_dotenv()
logger = dc.setup_logger()


# ===================================================== #
#                                                       #
#                 U N I T   T E S T S                   #
#                                                       #
# ===================================================== #

# ---------------------------------------- #
# geckoterminal_metadata_search() unit tests
# ---------------------------------------- #

@pytest.fixture
def api_params():
    """
    Fixture to provide valid API parameters for testing.

    Returns:
        dict: A dictionary containing valid blockchain and address values.
    """
    return {
        "blockchain": "ethereum",
        "address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"  # Example address (UNI token)
    }

@pytest.fixture
def mock_successful_response():
    """
    Fixture to create a mock successful API response.

    Returns:
        Mock: A mock object simulating a successful requests.Response.
    """
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = json.dumps({
        "data": {
            "id": "ethereum_0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
            "type": "token",
            "attributes": {
                "name": "Uniswap",
                "symbol": "UNI",
                "address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
            }
        }
    })
    return mock_response

@pytest.mark.unit
@patch('geckoterminal_coin_metadata.requests.get')
def test_ping_geckoterminal_api_successful(mock_get, api_params, mock_successful_response):
    """
    Test the ping_geckoterminal_api function for a successful API call.

    This test verifies that the function correctly handles a successful API response,
    returns the expected data, and doesn't attempt any retries.

    Args:
        mock_get (MagicMock): Mocked requests.get function.
        api_params (dict): Fixture providing API parameters.
        mock_successful_response (Mock): Fixture providing a mock successful response.
    """
    # Arrange
    mock_get.return_value = mock_successful_response

    # Act
    response_data, status_code = gtm.ping_geckoterminal_api(
        api_params['blockchain'],
        api_params['address']
    )

    # Assert
    assert status_code == 200
    assert 'data' in response_data
    assert response_data['data']['id'] == f"{api_params['blockchain']}_{api_params['address']}"
    assert response_data['data']['attributes']['name'] == "Uniswap"
    assert response_data['data']['attributes']['symbol'] == "UNI"

    # Verify that the API was called only once (no retries)
    mock_get.assert_called_once_with(
        f"https://api.geckoterminal.com/api/v2/networks/{api_params['blockchain']}/tokens/{api_params['address']}",
        timeout=30
    )
@pytest.fixture
def mock_rate_limited_response():
    """
    Fixture to create a mock rate-limited API response.

    Returns:
        Mock: A mock object simulating a rate-limited requests.Response.
    """
    mock_response = Mock()
    mock_response.status_code = 429
    mock_response.text = json.dumps({"error": "Rate limit exceeded"})
    return mock_response

@pytest.fixture
def mock_error_response():
    """
    Fixture to create a mock error API response.

    Returns:
        Mock: A mock object simulating an error requests.Response.
    """
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.text = json.dumps({"error": "Not found"})
    return mock_response

@pytest.mark.unit
@patch('geckoterminal_coin_metadata.requests.get')
@patch('geckoterminal_coin_metadata.time.sleep')
def test_ping_geckoterminal_api_rate_limit_success(mock_sleep, mock_get, api_params, mock_rate_limited_response, mock_successful_response):
    """
    Test the ping_geckoterminal_api function for a rate-limited API call that succeeds on retry.

    This test verifies that the function correctly handles a rate-limited response,
    retries the request, and succeeds on the second attempt.

    Args:
        mock_sleep (MagicMock): Mocked time.sleep function.
        mock_get (MagicMock): Mocked requests.get function.
        api_params (dict): Fixture providing API parameters.
        mock_rate_limited_response (Mock): Fixture providing a mock rate-limited response.
        mock_successful_response (Mock): Fixture providing a mock successful response.
    """
    # Arrange
    mock_get.side_effect = [mock_rate_limited_response, mock_successful_response]

    # Act
    response_data, status_code = gtm.ping_geckoterminal_api(
        api_params['blockchain'],
        api_params['address']
    )

    # Assert
    assert status_code == 200
    assert 'data' in response_data
    assert response_data['data']['id'] == f"{api_params['blockchain']}_{api_params['address']}"

    # Verify that the API was called twice and sleep was called once
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once_with(30)

@pytest.mark.unit
@patch('geckoterminal_coin_metadata.requests.get')
@patch('geckoterminal_coin_metadata.time.sleep')
def test_ping_geckoterminal_api_max_retries_exceeded(mock_sleep, mock_get, api_params, mock_rate_limited_response):
    """
    Test the ping_geckoterminal_api function when max retries are exceeded.

    This test verifies that the function correctly handles multiple rate-limited responses,
    retries the maximum number of times, and returns the last rate-limited response.

    Args:
        mock_sleep (MagicMock): Mocked time.sleep function.
        mock_get (MagicMock): Mocked requests.get function.
        api_params (dict): Fixture providing API parameters.
        mock_rate_limited_response (Mock): Fixture providing a mock rate-limited response.
    """
    # Arrange
    mock_get.return_value = mock_rate_limited_response

    # Act
    response_data, status_code = gtm.ping_geckoterminal_api(
        api_params['blockchain'],
        api_params['address']
    )

    # Assert
    assert status_code == 429
    assert 'error' in response_data
    assert response_data['error'] == "Rate limit exceeded"

    # Verify that the API was called 3 times (initial + 2 retries) and sleep was called twice
    assert mock_get.call_count == 3
    assert mock_sleep.call_count == 3

@pytest.mark.unit
@patch('geckoterminal_coin_metadata.requests.get')
def test_ping_geckoterminal_api_error_response(mock_get, api_params, mock_error_response):
    """
    Test the ping_geckoterminal_api function for an error API response.

    This test verifies that the function correctly handles an error response
    and returns the error data without attempting retries.

    Args:
        mock_get (MagicMock): Mocked requests.get function.
        api_params (dict): Fixture providing API parameters.
        mock_error_response (Mock): Fixture providing a mock error response.
    """
    # Arrange
    mock_get.return_value = mock_error_response

    # Act
    response_data, status_code = gtm.ping_geckoterminal_api(
        api_params['blockchain'],
        api_params['address']
    )

    # Assert
    assert status_code == 404
    assert 'error' in response_data
    assert response_data['error'] == "Not found"

    # Verify that the API was called only once (no retries)
    mock_get.assert_called_once()
