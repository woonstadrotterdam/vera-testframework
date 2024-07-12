import csv

import requests


def get_csv_from_release(
    release_tag: str = "latest",
    repo: str = "Aedes-datastandaarden/vera-referentiedata",
    file_path: str = "Referentiedata.csv",
    timeout: int = 10,
) -> list[dict[str, str]]:
    """
    Download a CSV file from a specific release on GitHub and process it.

    Args:
        release_tag (str): The tag of the release. If "latest", fetch the latest release. Default is "latest".
        repo (str): GitHub repository in the format 'owner/repo'. Default is "Aedes-datastandaarden/vera-referentiedata".
        file_path (str): Path to the CSV file in the repository. Default is "Referentiedata.csv".
        timeout (int): The timeout in seconds for the HTTP request. Default is 10 seconds.

    Returns:
        list[dict[str, str]]: A list of dictionaries, where each dictionary represents a row in the CSV file.

    Raises:
        Exception: If the request to download the CSV file fails.
    """
    if release_tag == "latest":
        release_tag = get_latest_release_tag(repo, timeout)

    url = f"https://raw.githubusercontent.com/{repo}/{release_tag}/{file_path}"
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        # Decode the response content to a string
        content = response.text.splitlines()
        csv_reader = csv.DictReader(content, delimiter=";")
        rows = [row for row in csv_reader]
        return rows
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            raise Exception(
                f"CSV file not found for release tag '{release_tag}'. Please check the release tag and try again."
            )
        else:
            raise Exception(f"Failed to download file: {e}")


def get_latest_release_tag(repo: str, timeout: int = 10) -> str:
    """
    Get the latest release tag for a GitHub repository.

    Args:
        repo (str): GitHub repository in the format 'owner/repo'.
        timeout (int): The timeout in seconds for the HTTP request. Default is 10 seconds.

    Returns:
        str: The tag of the latest release.
    """
    url = f"https://api.github.com/repos/{repo}/releases/latest"
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    latest_release = response.json()
    return str(latest_release["tag_name"])
