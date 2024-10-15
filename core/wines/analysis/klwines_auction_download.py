import xml.etree.ElementTree as ET
from io import StringIO

import requests
from fix_unicode import fix_bad_unicode
from openpyxl import Workbook


def download_and_parse_xml():
    url = "https://klwprdshopfeed.blob.core.windows.net/winesearcher/auction.xml"

    # Download the XML data
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to download XML. Status code: {response.status_code}")

    # Parse the XML
    xml_data = StringIO(response.text)
    tree = ET.parse(xml_data)
    root = tree.getroot()

    # Prepare data for Excel
    headers = ["name", "vintage", "url", "price", "unit-size"]
    data = [headers]

    for row in root.findall(".//row"):
        wine_data = {}
        for element in row:
            if element.tag == "name":
                wine_data[element.tag] = fix_bad_unicode(element.text)
            else:
                wine_data[element.tag] = element.text
        data.append([wine_data.get(header, "") for header in headers])

    # Create Excel workbook and add data
    wb = Workbook()
    ws = wb.active
    ws.title = "KL Wines Auction Data"

    for row in data:
        ws.append(row)

    # Save the Excel file
    output_file = "klwines_auction_data.xlsx"
    wb.save(output_file)

    print(f"Data has been saved to {output_file}")


if __name__ == "__main__":
    download_and_parse_xml()
