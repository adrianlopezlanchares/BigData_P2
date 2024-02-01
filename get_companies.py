import requests
from bs4 import BeautifulSoup


def get_companies_cik(sector: str = "Consumer Discretionary") -> dict:   
    """Returns a dictionary with the symbols (tickers) and CIKs,
    of the companies in 'sector' (preset to consumer_discretionary)
    by webscraping the Wikipedia page of the S&P 500 companies.

    Args:
        sector: Sector of the companies to get the CIKs from.
    Returns:
        dict: Dictionary with the CIKs of the companies in 'tickers'
    """

    link = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    # Get the HTML code from the link
    html = requests.get(link).text

    # Create a BeautifulSoup object from the HTML
    soup = BeautifulSoup(html, "html.parser")

    # Get the table with the data
    table = soup.find("table", {"id": "constituents"})
    
    # Get the body of the table
    table_body = table.find("tbody")
    rows = table_body.find_all("tr")

    # Indexes of the columns we want
    symbol_index = 0
    GICS_sector_index = 2
    CIK_index = 6
    
    # Get the rows of the table
    rows = table_body.find_all("tr")

    # Create the dictionary with the symbols and CIKs
    companies_ciks_dict = {}

    # Select the rows that belong to the sector (the first row is the header)
    for row in rows[1:]:
        if row.find_all("td")[GICS_sector_index].text.strip() == sector:
            # Get the columns of each row
            cols = row.find_all("td")

            # Get the ticker
            symbol = cols[symbol_index].text.strip()

            # Get the CIK
            CIK = cols[CIK_index].text.strip()

            # Print the ticker and the CIK
            companies_ciks_dict[symbol] = CIK

    return companies_ciks_dict
