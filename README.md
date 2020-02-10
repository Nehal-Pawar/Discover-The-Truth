# Discover-The-Truth

#### Getting birdview on news

Slides are here
Video demo here
## Introduction to Project Idea
People get manipulated by different news channel by presenting news from different point of views skipping important points which stay in their favour. The idea of this project is to get news from different sources and put them together in one place where everyone can read everyones point of view and get a complete picture. 

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
1. Add Real time news and twitter APIs to make app reaal time. Also add search APIs for news,twitter and youtube
2. Search Bar: Add serach bar to the UI that should take keywords and a collection of multiple keywords for same topic


