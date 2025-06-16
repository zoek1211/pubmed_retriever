import xml.etree.ElementTree as ET
import requests
import concurrent.futures
import time
import re
from unidecode import unidecode

##############################################
# PART 1: GET TITLES FROM XML FILE
##############################################

# Parse xml into element tree
tree = ET.parse('4020a1-datasets.xml')
root = tree.getroot()

# Find "ArticleTitle" tags and store them in a list 
titles = []

for article_title in root.findall(".//ArticleTitle"):
    if article_title is not None and article_title.text:
        titles.append(article_title.text)


##############################################
# PART 2: FETCH CANDIDATE PMIDS FOR EACH TITLE
##############################################

# identifies and authenticates a user
API_KEY = "1deb1aa4f92314b69cb78eed237df2811a09"

# defines the base URL for PubMed
BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

# creates an empty list called failed_titles to store the name of the titles from the title list that failed
failed_titles = []


# Preprocessing for Re: titles (partial matching)
def preprocess_title(title):
    # Remove single and double quotes, special characters, extra whitespace,
    # remove trailing period, and remove content inside parentheses or double quotes.
    title = re.sub(r"[\'\"]", "", title)            # Remove quotes
    title = re.sub(r"[^\w\s,;.:]", " ", title)         # Remove special characters (except some punctuation)
    title = re.sub(r"\s+", " ", title).strip()         # Collapse whitespace
    if title.endswith('.'):
        title = title[:-1].strip()                     # Remove trailing period
    title = re.sub(r"\(.*?\)", "", title)              # Remove content inside parentheses
    title = re.sub(r"\".*?\"", "", title)              # Remove content inside double quotes
    return title


# Fetch PMIDs for a given title
def fetch_pmids(title):

    keywords = ' '.join(title.split()[:10]) # Takes the first 10 words of a title as keywords

    # Defines the parameters for the POST request
    params = {
        "db": "pubmed",
        "term": keywords,  # Use keywords for flexible search
        "retmax": 5,
        "retmode": "xml",
        "field": "title",
        "api_key": API_KEY,
        "usehistory": "y",
        "sort": "relevance"
    }

    # Executes the POST request
    response = requests.post(BASE_URL, data=params)
    response.raise_for_status()

    # Parses the XML string from response.text to create an ElementTree object called root.
    # If there are pmids in the list called pmids then it returns them, otherwise returns an empty list if there is no pmids.
    root = ET.fromstring(response.text)
    pmids = [elem.text for elem in root.findall(".//Id")]
    if pmids:
        return pmids
    return []


# This function tries up to a predetermined number of times (retries) to retrieve PMIDs for a given title. It waits for a predetermined period of time (wait) before attempting again in the event of a 429 Too Many Requests error.
# It alerts you if the problem keeps happening after trying a set number of times. It then adds the title to the failed_titles list and returns an empty list for all other HTTP errors or exceptions.
def fetch_pmids_with_retry(title, retries=5, wait=1):
    for attempt in range(retries):
        try:
            return fetch_pmids(title)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                if attempt < retries - 1:
                    time.sleep(wait)
                    continue  # Continue to the next attempt
                else:
                    raise  # No more retries, re-raise the error
            else:
                failed_titles.append(title)
                return []
        except Exception as e:
            failed_titles.append(title)
            return []


# Build mapping: title -> PMID
def submit(titles):

    # Initializes a dictionary to store the mapping of titles to pmids
    mapping = {}

    # Creates a pool (group) of 10 worker threads to handle tasks at the same time
    # 'executor' assigns tasks to threads and keeps track of their progress
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

        # Submit tasks to fetch the pmids for each preprocessed title
        # 'executor.submit' schedules the function to be executed and returns a placeholder for the result of the task (future object)
        future_to_title = {executor.submit(fetch_pmids_with_retry, preprocess_title(t)): t for t in titles}

        # Loops through each completed task as soon as finished
        # 'concurrent.futures.as_completed' yields task values (futures) as they complete
        for future in concurrent.futures.as_completed(future_to_title):
            # Gets the original title associated with the completed task (future)
            orig_title = future_to_title[future]


            try:
                # Try to get the result (PMID list) from the completed task. This will wait until the task is complete and the result is available
                pmid_list = future.result()

                # check if pmids are found
                if pmid_list:
                    # If pmids are found, store the original title as the key and the list of pmids as the value in the mapping dictionary
                    mapping[orig_title] = pmid_list

                else:
                    # If no pmids are found, store an empty list in the mapping dictionary for this title and print a message
                    print(f"Title: {orig_title}\nNo PMIDs found.")
                    mapping[orig_title] = []

            except Exception as e:
                # if an exception occurs during task execution, print an error message and store an empty list for this title in the mapping dictionary to indicate failure
                print(f"Request failed for '{orig_title}': {e}")
                mapping[orig_title] = []

    # return the final dictionary that maps each original title to its correct corresponding list of pmids
    return mapping


# This function will batch the titles into groups of a given size
def batch_titles(titles, batch_size):
    for i in range(0, len(titles), batch_size):
        yield titles[i:i + batch_size]



# Convert generator to a list so we can index it.
batched = list(batch_titles(titles, 10))

# Initialize an empty dictionary to store the final mapping of titles to pmids
final_mapping_of_ids = {}

# Uses the performance counter to store the start time
start_time = time.perf_counter()

# Get the number of batches
loops = len(batched)

# Loop through each batch
for i in range(loops):
    # Prints the current batch number being processed out of the ttal number of batches
    print(f"Processing batch {i+1} of {len(batched)}")
    # Submits the current batch for processing and gets the mapping og titles to pmids for this batch
    batch_mapping = submit(batched[i])
    # Updates the final mapping dictionary with the results from current batch
    final_mapping_of_ids.update(batch_mapping)
    time.sleep(0.8)  # Delay between batches to avoid rate limiting
    print("\n")

# Prints the total number of titles processsed out of the total number of input titles
print("\nTotal titles processed:", len(final_mapping_of_ids), "/", len(titles))


#############################################
# PART 3: CHECK MATCHES AND EXTRACT XML DATA
#############################################

EPOST_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/epost.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

# This function will take a string and remove accents from it
def remove_accents(input_str):
    return unidecode(input_str)

# Normalize a title for matching
def normalize_title(title):
    # Simple normalization: lowercase, trim, and remove trailing punctuation.
    if not title:
        return ""
    title = re.sub(r"[.,;:!?]+$", "", title)  # Remove trailing punctuation at the very end
    title = title.lower().strip()
    title = title = remove_accents(title)  # Remove accents/diacritics.
    title = re.sub(r"\s+", " ", title)  # Normalize whitespace
    return title

# This function will submit all pmids found to the history server and return the WebEnv and QueryKey
def post_pmids_to_history(pmids):
    """
    EPost all PMIDs to the History Server, return (WebEnv, QueryKey).
    """
    pmid_str = ",".join(pmids)
    params = {
        "db": "pubmed",
        "api_key": API_KEY
    }
    data = {"id": pmid_str}
    resp = requests.post(EPOST_URL, params=params, data=data)
    resp.raise_for_status()

    root = ET.fromstring(resp.text)
    webenv = root.findtext("WebEnv")
    query_key = root.findtext("QueryKey")
    if not webenv or not query_key:
        raise RuntimeError("EPost did not return WebEnv/QueryKey.\n" + resp.text)

    return webenv, query_key


# This function will fetch the article details for a given WebEnv and QueryKey
def efetch_bulk(webenv, query_key, count):
    
    # Retrieve 'count' articles from the History Server using the provided WebEnv and QueryKey.
    # Returns a dictionary mapping each PMID to its fetched article title, and raw XML for storage.
    
    params = {
        "db": "pubmed",
        "api_key": API_KEY,
        "WebEnv": webenv,
        "query_key": query_key,
        "usehistory": "y",
        "retmode": "xml",
        "retmax": count
    }
    response = requests.get(EFETCH_URL, params=params)
    response.raise_for_status()
    root = ET.fromstring(response.text)

    mapping = {}
    for article in root.findall("PubmedArticle"):
        pmid = article.findtext(".//PMID")
        if pmid:
            mapping[pmid] = article  # Store the entire article instead of just the title


    return mapping

# Function to build minimal XML
def build_minimal_xml(article):
    pmid_el = article.find(".//PMID")
    title_el = article.find(".//ArticleTitle")
    pmid_val = pmid_el.text if pmid_el is not None else "N/A"
    title_val = title_el.text if title_el is not None else "N/A"

    new_xml = "    <PubmedArticle>\n"
    new_xml += f"        <PMID>{pmid_val}</PMID>\n"
    new_xml += f"        <ArticleTitle>{title_val}</ArticleTitle>\n"
    new_xml += "    </PubmedArticle>\n"
    return new_xml


# Function to process each query title
def process_query_title(query_title, candidate_pmids, candidate_mapping):
    norm_query = normalize_title(query_title)
    best_pmid = None
    local_xml_data = ""
    norm_candidate = ""
    best_article = None

    for pmid in candidate_pmids:
        if pmid == "0":
            continue

        article = candidate_mapping.get(pmid)
        if article is None:
            continue
        fetched_title = article.findtext(".//ArticleTitle")

        norm_candidate = normalize_title(fetched_title)

        if norm_candidate == norm_query:
            best_pmid = pmid
            best_article = article
            break

    if best_pmid:
        all_pmids.append(best_pmid)
        local_xml_data += build_minimal_xml(best_article)
    else:
        print(f"Title: '{query_title}' -> No matching PMID found.\n")

    return local_xml_data

# This function fetches the article title for a given PMID and checks if it matches the query title
def check_matches(query_to_candidates):

    # Given a dictionary mapping each query title to its list of candidate PMIDs,
    # this function:
    #   1. Combines all candidate PMIDs in the batch.
    #   2. Performs one bulk EFetch call (via efetch_bulk) to get all candidate article titles.
    #   3. For each query title, loops through its candidate PMIDs (using the bulk‐fetched mapping)
    #      and checks if the normalized candidate title equals the normalized query title.
    #      It prints the best matching PMID (if found) or a message if none match.

    xml_data = "<PubmedArticleSet>\n"

    # Combine all candidate PMIDs from the batch
    all_candidate_pmids = set()
    for candidate_list in query_to_candidates.values():
        all_candidate_pmids.update(candidate_list)
    all_candidate_pmids = list(all_candidate_pmids)

    # Post all PMIDs to the History Server:
    try:
        webenv, query_key = post_pmids_to_history(all_candidate_pmids)
        print(f"WebEnv: {webenv}")
        print(f"QueryKey: {query_key}")
    except Exception as e:
        print("Error in EPost:", e)
        exit(1)

    # Use one bulk EFetch request to retrieve article titles for all candidate PMIDs
    candidate_mapping = efetch_bulk(webenv, query_key, len(all_candidate_pmids))


    # Thread pool to process compare titles for matches concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for query_title, candidate_pmids in query_to_candidates.items():
            futures.append(executor.submit(process_query_title, query_title, candidate_pmids, candidate_mapping))

        for future in concurrent.futures.as_completed(futures):
            xml_data += future.result()

    xml_data += "</PubmedArticleSet>"
    return xml_data



time.sleep(1)  # Delay before checking matches

# Check matches for each title and store correct pmids as well as the xml data to a file
all_pmids = []

print("\nChecking matches for each title:")
xml_result = check_matches(final_mapping_of_ids)
print(xml_result)

print("\nTotal matched pmids:", len(all_pmids), "/", len(titles))


#############################################
# PART 4: WRITE XML DATA TO A FILE
#############################################

# Function to write minimal XML data to a file
def write_xml_to_file(xml_data):
    # Save the XML to a file
    output_filename = "group5's result.xml"
    with open(output_filename, "w", encoding="utf-8") as file:
        file.write(xml_data)

    print(f"Minimal XML saved to {output_filename}")


end_time = time.perf_counter()
elapsed = end_time - start_time

print(f"Total time taken for the loop: {elapsed:.2f} seconds")
# print(xml_result)

#writes data to the file
write_xml_to_file(xml_result)