import os
import requests

dir_path = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(dir_path, 'proxy_http_ip.txt')


def load_proxies_from_file():
    """
    Load proxies from a given file and return as a list.

    Args:
    - filepath (str): The path to the proxy list file.

    Returns:
    - List[str]: A list of proxy strings.
    """
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

# proxies = load_proxies_from_file()
# print(proxies)


# proxies = {
#     "http": "https://DEO06D7RI:j8oq7wya@45.132.184.60:8080",
#     "https": "https://DEO06D7RI:j8oq7wya@45.132.184.60:8080"
# }
#
# try:
#     response = requests.get("https://ipinfo.io", proxies=proxies, timeout=10)
#     if response.status_code == 200:
#         print(response.text)
#     else:
#         print(f"Received unexpected status code {response.status_code}.")
# except requests.RequestException as e:
#     print(f"Error using proxy: {e}")

# username = "DEO06D7RI"
# password = "j8oq7wya"
#
# with open(file_path, 'r') as f:
#     ips = f.readlines()
#
# formatted_ips = [f"https://{username}:{password}@{ip.strip()}" for ip in ips]
#
# with open('proxy_http_ip.txt', 'w') as f:
#     for proxy in formatted_ips:
#         f.write(proxy + "\n")
#
# print("Format conversion complete!")
