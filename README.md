# WikipediaCrawler
Python Wikipedia Crawler with the intent to create a biomedical dictionary in portuguese

Not all code for this solution, since it used resources from the Institute of Electronics and Informatics Engineering of Aveiro, such as an UMLS database.

## Category Crawling

wikicrawler.py crawls a set of categories until a certain depth using the Wikipedia API, in my case there were 4 major biomedical categories.
The information extracted from the page were it's links and the ID's written in it's infobox.

## Database Access

Validation of the ID found against the UMLS database. If no ID was found, there was an attemp to match an entry on the database by name, only if it was not ambigous.

## Portuguese link

getptname.py will search for the portuguese name using the Wikipedia API
