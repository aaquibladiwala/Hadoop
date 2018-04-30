import sys

def map_output(file):

    """ Return an iterator for key, value pair extracted from file
    Input format:  key \t value
    Output format: (key, value)
    """
    for line in file:
        yield line.strip().split("\t", 1)
def video_reducer():
    """ 
    in hadoop streaming multiple keys maybe given to the reducer
        e.g. video1, country_cat1
             video1, country_cat2
             video2, country_cat2
             video2, country_cat3
             video3, country_cat1
    Input format: video_id \t country_category
    Output format: category \t country=count
    """
    country1 = "GB"
    country2 = "US"
    countCategoryCountry1 = {}
    countCategoryCountry2 = {}
    CommonCount = {}
    category = []
    current_video = ""
    country_cat = []
    for video, countrycat in map_output(sys.stdin):
        # Check if the video read is the same as the video currently being processed
        if current_video != video:
            # If this is the first line (indicated by the fact that current_video will have the default value of "",
            if current_video != "":
                for c in country_cat:

                    country, cat = c.strip().split(";")
                    if cat not in category:
                        category.append(cat)
                        

                    if country == country1:

                        countCategoryCountry1[cat] = countCategoryCountry1.get(cat,0) + 1

                        appear_country1 = 1

                    if country == country2:

                        countCategoryCountry2[cat] = countCategoryCountry2.get(cat,0) + 1

                        appear_country2 = 1

                    if ((appear_country1 == 1) and (appear_country2 == 1)):

                        CommonCount[cat] = CommonCount.get(cat,0) + 1

                

            # Reset the video being processed and clear the country_cat list for the new video
            current_video = video
            country_cat = []
            appear_country1 = appear_country2 = 0

            

        if countrycat not in country_cat:
            country_cat.append(countrycat)
    # We need to output video-country_cat count for the last video. However, we only want to do this if the for loop is called.
    if current_video != "":
        for c in country_cat:
            country, cat = c.strip().split(";")
            if cat not in category:
                category.append(cat)
            if country == country1:
                countCategoryCountry1[cat] = countCategoryCountry1.get(cat,0) + 1
                appear_country1 = 1
            if country == country2:
                countCategoryCountry2[cat] = countCategoryCountry2.get(cat,0) + 1
                appear_country2 = 1
            if ((appear_country1 ==1) and (appear_country2 == 1)):
                CommonCount[cat] = CommonCount.get(cat,0) + 1

    for cat in category:
        corel = int(CommonCount.get(cat,0))
        denom = int(countCategoryCountry1.get(cat,0))
        if denom == 0:
          cat_perccent = 0
          
        else: 
            cat_perccent = round(((float(corel) / float(denom)) * 100),1)
            output = str(cat) + ": Total in " + country1 + ": " + str (denom) + " , " + str(cat_perccent) + " % in " + country2
            print (output)
           # print (cat , " : Total in ",country1," ", cat_perccent,"% in " , country2)
if __name__ == "__main__":

    video_reducer()

