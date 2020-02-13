# Discover-The-Truth

#### Getting birdview on news

Slides are [here](https://docs.google.com/presentation/d/1c45UHFSGbUHCeABy1eteTTe3eDqkZjBM/edit?dls=true)

Video demo [here](https://www.youtube.com/watch?v=5J-iZ33vWjg)
## Introduction to Project Idea
People get manipulated by different news channel. Different News media are presenting news from different point of views. Some of which is true and some of it is news channels personal view. In all this biased news reporting important news get shadowed and biased is formed by highliting same news from a different point of view to manipulate people in desired direction. The idea of this project is to get news from different sources and put them together in one place where everyone can read everyones point of view and get a complete picture. It also includes public opinion from twitter.

## DataSet
Kaggal News : https://www.kaggle.com/snapcrack/all-the-news
This dataset contains news for 2015-2017 from 14 different American news channel. Dataset is in comma seperated CSV format.
id, title, Publication name, Author name, Date of publication, Year of publication, Month of publication, URL for article (not available for all articles), Article content
#
Twitter :https://archive.org/search.php?query=archiveteam-twitter-stream
This data set contains tweets from year 2017. Dataset format is json.
## Architecture
![github-small](https://github.com/Nehal-Pawar/Discover-The-Truth/blob/master/Images/Datapipeline.PNG)

### Engineering Challenge
Challenge 1: Collect data from multiple sources and arrange in same format. Data from news is in comma seperated .csv format.
Twitter is in json format.

Challenge 2: Feed data to a Database that accepts key value pair with a fature that can accept new columns later.
It should also support search over a column 
## Future Scope
1. Add Real time news and twitter APIs to make app real time. Also add search APIs for news,twitter and youtube
2. Increase the range from America's news to World/International News
3. Do ordering of the news and tweets and youtube videos in an unbiased way
4. Improve search algorithm by using google keyword planner
5. Search Bar: Add serach bar to the UI that should take keywords and a collection of multiple keywords (from google keyword planner) for same topic


