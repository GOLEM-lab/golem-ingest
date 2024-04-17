# Ingesting Archive of Our Own Archives into Elasticsearch
Scripts to ingest stories in Archive of our Own archives into Elasticsearch. The input archives were retrieved from https://archive.org/details/AO3_final_location in the Fall of 2022. 

## Requirements
Install the requirements with `pip install -r requirements.txt`.

## Scripts


Three scripts, found under `ao3_to_elastic` were used in succession (with some time in between) to populate the Elasticsearch index. Run them with `python <script_filename>`.

1. `ingest.py` performed the inital ingest, parsing metadata from the SQLite file from the AO3 archive and combining it with the stories' contents from the corresponding EPUB files.
2. `add_characters.py` parses the characters from the header found in each EPUB file and adds it as a new field in the index.
3. `parse_genre.py` renames and splits some fields to bring them more in line with the header of a story on the AO3 website.

Note that we only ingest the stories listed in the SQLite file. Potentially there are more stories in the zip files whose metadata could have been parsed from their headers.
