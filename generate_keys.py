import json
import requests
import sys
import time

from hashlib import sha3_256
from multiprocessing import Pool
from pymongo import MongoClient
from utils.string_processor import preprocess_author_name, preprocess_journal_title
from xylose.scielodocument import Article, Citation


def _get_cleaned_first_author_name(first_author: dict):
    if first_author:
        initial = ''
        lastname = ''

        fa_surname = preprocess_author_name(first_author.get('surname', ''))
        fa_givennames = preprocess_author_name(first_author.get('given_names', ''))

        if fa_surname:
            lastname = fa_surname.split(' ')[-1]
        
        if fa_givennames:
            initial = fa_givennames[0]
        
        cleaned_first_author_name = ' '.join([initial, lastname]).strip()

        if cleaned_first_author_name:
            return cleaned_first_author_name.lower()


def _get_cleaned_volume(volume: str):
    if volume:
        return preprocess_author_name(volume).lower()


def _get_cleaned_issue(issue: str):
    if issue:
        return preprocess_author_name(issue).lower()


def _get_cleaned_first_page(first_page: str):
    if first_page:
        return preprocess_author_name(first_page).lower()


def _extract_citation_key(citation: Citation, cit_full_id: str, cit_standardized_data):
    data = {
        'cleaned_first_author': '',
        'cleaned_title': '',
        'cleaned_journal_title': '',
        'cleaned_volume': '',
        'cleaned_issue': '',
        'cleaned_first_page': ''
    }

    if cit_standardized_data:
        data['cleaned_journal_title'] = cit_standardized_data['cit_official_journal_title'].lower()
    else:
        cleaned_journal_title = preprocess_journal_title(citation.source).lower()
        if cleaned_journal_title:
            data['cleaned_journal_title'] = cleaned_journal_title

    cleaned_title = preprocess_author_name(citation.title()).lower()
    if cleaned_title:
        data['cleaned_title'] = cleaned_title

    cleaned_first_author = _get_cleaned_first_author_name(citation.first_author)
    if cleaned_first_author:
        data['cleaned_first_author'] = cleaned_first_author

    cleaned_volume = _get_cleaned_volume(citation.volume)
    if cleaned_volume:
        data['cleaned_volume'] = cleaned_volume

    cleaned_issue = _get_cleaned_issue(citation.issue)
    if cleaned_issue:
        data['cleaned_issue'] = cleaned_issue

    cleaned_first_page = _get_cleaned_first_page(citation.start_page)
    if cleaned_first_page:
        data['cleaned_first_page'] = cleaned_first_page

    if cleaned_first_author and cleaned_first_page and cleaned_issue and cleaned_volume and cleaned_title and cleaned_journal_title:
        return data


def _mount_citation_id(citation: Citation, collection_acronym):
    cit_id = citation.data['v880'][0]['_']
    cit_full_id = '{0}-{1}'.format(cit_id, collection_acronym)
    return cit_full_id


def extract_citations_ids_keys(document: Article, standardizer):
    citations_ids_keys = []

    if document.citations:
        for cit in [c for c in document.citations if c.publication_type == 'article']:
            cit_full_id = _mount_citation_id(cit, document.collection_acronym)

            cit_standardizerd_data = standardizer.find_one({'_id': cit_full_id})

            cit_keys = _extract_citation_key(cit, cit_full_id, cit_standardizerd_data)
            if cit_keys:
                cit_sha3_256 = sha3_256('-'.join([cit_keys[k] for k in sorted(cit_keys)]).strip().encode()).hexdigest()
                citations_ids_keys.append((cit_full_id, cit_keys, cit_sha3_256))

    return citations_ids_keys


def convert_to_mongodoc(data):
    mgdocs = {}
    for doc_id, citations_data in [d for d in data if d]:
        for cit in citations_data:
            cit_full_id = cit[0]
            cit_keys = cit[1]
            cit_sha3_256 = cit[2]

            if cit_sha3_256 not in mgdocs:
                mgdocs[cit_sha3_256] = {'cit_full_ids': [], 'citing_docs': [], 'cit_keys': cit_keys}

            mgdocs[cit_sha3_256]['cit_full_ids'].append(cit_full_id)
            mgdocs[cit_sha3_256]['citing_docs'].append(doc_id)

    return mgdocs


def save_data_to_mongo(data):
    # TODO: remove hardcoded MongoConnection
    mongo_data = convert_to_mongodoc(data)

    mdb = MongoClient()
    writer = mdb['citations']['deduplicated']
    
    for cit_sha3_256 in mongo_data:
        new_doc = mongo_data[cit_sha3_256]    
        writer.update_one(
            filter={'_id': str(cit_sha3_256)},
            update={
                '$set': new_doc['cit_keys'],
                '$addToSet': {
                    'cit_full_ids': {'$each': new_doc['cit_full_ids']},
                    'citing_docs': {'$each': new_doc['citing_docs']}}},
            upsert=True)

    mdb.close()


def parallel_extract_citations_ids_keys(doc_id):
    client = MongoClient(maxPoolSize=None)
    articles = client['ami']['articles-issues']
    standardizer = client['citation']['standardizer']

    raw = articles.find_one({'_id': doc_id})
    doc = Article(raw)

    citations_keys = extract_citations_ids_keys(doc, standardizer)
    if citations_keys:
        return (('-'.join([doc.publisher_id, doc.collection_acronym]), citations_keys))


print('[1] Getting documents\' ids...')
start = time.time()

# TODO: remove hardcoded MongoConnection
main_client = MongoClient(maxPoolSize=None)
docs_ids = [x['_id'] for x in main_client['ami']['articles-issues'].find({}, {'_id': 1})]
total_docs = len(docs_ids)
main_client.close()

end = time.time()
print('\tDone after %.2f seconds' % (end - start))

start = time.time()
# TODO: remove hardcoded interval
interval = 2000
print('[2] Generating keys...')
chunks = range(0, total_docs, interval)
for slice_start in chunks:
    slice_end = slice_start + interval
    if slice_end > total_docs:
        slice_end = total_docs

    print('\t%d to %d' % (slice_start, slice_end))
    # TODO: remove hardcoded number of Threads
    with Pool(12) as p:
        results = p.map(parallel_extract_citations_ids_keys, docs_ids[slice_start:slice_end])

    save_data_to_mongo(results)
end = time.time()
print('\tDone after %.2f seconds' % (end - start))
