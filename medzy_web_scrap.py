import requests
import bs4
import prettify
from bs4 import BeautifulSoup

url = 'https://example.com'
response = requests.get("https://medzypharma.co.in")
soup = BeautifulSoup(response.text, 'html.parser')

# Extract data
data = soup.find_all('p')
print(data)
