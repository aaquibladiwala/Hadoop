import sys
import csv
def mapper():
    country1 = "GB"
    country2 = "US"
    for line in sys.stdin:
        csv_list = list(csv.reader([line]))
        video_id = csv_list[0][0]
        category = csv_list[0][5]
        country = csv_list[0][17]
        if (country == country1 or country == country2):
            country_cat = country + ';' + category
            print("{}\t{}".format(video_id, country_cat))


if __name__ == "__main__":
    mapper()
