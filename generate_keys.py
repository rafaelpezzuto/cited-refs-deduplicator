import time

from hashlib import sha3_256
from multiprocessing import Pool
from pymongo import MongoClient
from utils.field_cleaner import get_cleaned_publication_year, get_cleaned_first_author_name, get_cleaned_volume, get_cleaned_issue, get_cleaned_first_page
from utils.string_processor import preprocess_author_name, preprocess_journal_title
from xylose.scielodocument import Article, Citation


def _extract_citation_key(citation: Citation, cit_standardized_data):
    data = {
        'cleaned_title': '',
        'cleaned_publication_year': '',
        'cleaned_first_author': '',
        'cleaned_journal_title': '',
        'cleaned_volume': '',
        'cleaned_issue': '',
        'cleaned_first_page': ''
    }

    cleaned_journal_title = ''

    if cit_standardized_data:
        cleaned_journal_title = cit_standardized_data['official-journal-title'][0].lower()
        if cleaned_journal_title:
            data['cleaned_journal_title'] = cleaned_journal_title

    if not cleaned_journal_title:
        cleaned_journal_title = preprocess_journal_title(citation.source).lower()
        if cleaned_journal_title:
            data['cleaned_journal_title'] = cleaned_journal_title

    cleaned_publication_year = get_cleaned_publication_year(citation.publication_date)
    if cleaned_publication_year:
        data['cleaned_publication_year'] = cleaned_publication_year

    cleaned_title = preprocess_author_name(citation.title()).lower()
    if cleaned_title:
        data['cleaned_title'] = cleaned_title

    cleaned_first_author = get_cleaned_first_author_name(citation.first_author)
    if cleaned_first_author:
        data['cleaned_first_author'] = cleaned_first_author

    cleaned_volume = get_cleaned_volume(citation.volume)
    if cleaned_volume:
        data['cleaned_volume'] = cleaned_volume

    cleaned_issue = get_cleaned_issue(citation.issue)
    if cleaned_issue:
        data['cleaned_issue'] = cleaned_issue

    cleaned_first_page = get_cleaned_first_page(citation.start_page)
    if cleaned_first_page:
        data['cleaned_first_page'] = cleaned_first_page

    return data


def _mount_citation_id(citation: Citation, collection_acronym):
    cit_id = citation.data['v880'][0]['_']
    cit_full_id = '{0}-{1}'.format(cit_id, collection_acronym)
    return cit_full_id


def _generate_hashes(cit_keys):
    t = cit_keys.get('cleaned_title')
    py = cit_keys.get('cleaned_publication_year')
    j = cit_keys.get('cleaned_journal_title')
    fau = cit_keys.get('cleaned_first_author')
    fp = cit_keys.get('cleaned_first_page')
    i = cit_keys.get('cleaned_issue')
    v = cit_keys.get('cleaned_volume')

    t_py_j_fau_v = ''
    t_py_j_fau_fp = ''
    t_py_j_fau_i = ''

    if t and py and j and fau:
        if v:
            t_py_j_fau_v = sha3_256(''.join(['TITLE' + t, 'YEAR' + py, 'JOURNAL' + j, 'FIRST_AUTHOR' + fau, 'VOLUME' + v]).encode()).hexdigest()
        if fp:
            t_py_j_fau_fp = sha3_256(''.join(['TITLE' + t, 'YEAR' + py, 'JOURNAL' + j, 'FIRST_AUTHOR' + fau, 'FIRST_PAGE' + fp]).encode()).hexdigest()
        if i:
            t_py_j_fau_i = sha3_256(''.join(['TITLE' + t, 'YEAR' + py, 'JOURNAL' + j, 'FIRST_AUTHOR' + fau, 'ISSUE' + i]).encode()).hexdigest()

    return t_py_j_fau_v, t_py_j_fau_fp, t_py_j_fau_i


def extract_citations_ids_keys(document: Article, standardizer):
    citations_ids_keys = []

    if document.citations:
        for cit in [c for c in document.citations if c.publication_type == 'article']:
            cit_full_id = _mount_citation_id(cit, document.collection_acronym)

            cit_standardized_data = standardizer.find_one({'_id': cit_full_id, 'status': {'$gt': 0}})
            cit_keys = _extract_citation_key(cit, cit_standardized_data)
            hv, hfp, hi = _generate_hashes(cit_keys)

            if hv:
                citations_ids_keys.append((cit_full_id, cit_keys, hv, 'vol'))
            if hfp:
                citations_ids_keys.append((cit_full_id, cit_keys, hfp, 'fp'))
            if hi:
                citations_ids_keys.append((cit_full_id, cit_keys, hi, 'issue'))

    return citations_ids_keys


def convert_to_mongodoc(data):
    mgdocs = {'vol': {}, 'fp': {}, 'issue': {}}
    for doc_id, citations_data in [d for d in data if d]:
        for cit in citations_data:
            cit_full_id = cit[0]
            cit_keys = cit[1]
            cit_sha3_256 = cit[2]
            cit_hash_mode = cit[3]

            if cit_sha3_256 not in mgdocs:
                mgdocs[cit_hash_mode][cit_sha3_256] = {'cit_full_ids': [], 'citing_docs': [], 'vol': [], 'fp': [], 'issue': [], 'cit_keys': {}}

            mgdocs[cit_hash_mode][cit_sha3_256]['vol'].append(cit_keys.get('cleaned_volume'))
            mgdocs[cit_hash_mode][cit_sha3_256]['fp'].append(cit_keys.get('cleaned_first_page'))
            mgdocs[cit_hash_mode][cit_sha3_256]['issue'].append(cit_keys.get('cleaned_issue'))
            mgdocs[cit_hash_mode][cit_sha3_256]['cit_full_ids'].append(cit_full_id)
            mgdocs[cit_hash_mode][cit_sha3_256]['citing_docs'].append(doc_id)

            for k in ['cleaned_title', 'cleaned_publication_year', 'cleaned_first_author', 'cleaned_journal_title']:
                mgdocs[cit_hash_mode][cit_sha3_256]['cit_keys'][k] = cit_keys[k]

    return mgdocs


def save_data_to_mongo(data):
    # TODO: remove hardcoded MongoConnection
    mongo_data = convert_to_mongodoc(data)

    for k, v in mongo_data.items():
        mdb = MongoClient()
        writer = mdb['citations']['dt2-' + k]

        for cit_sha3_256 in v:
            new_doc = v[cit_sha3_256]
            writer.update_one(
                filter={'_id': str(cit_sha3_256)},
                update={
                    '$set': {'cit_keys': new_doc['cit_keys']},
                    '$addToSet': {
                        'cit_full_ids': {'$each': new_doc['cit_full_ids']},
                        'citing_docs': {'$each': new_doc['citing_docs']},
                        'cit_vol': {'$each': new_doc['vol']},
                        'cit_issue': {'$each': new_doc['issue']},
                        'cit_first_page': {'$each': new_doc['fp']}
                    }
                },
                upsert=True)

        mdb.close()


def parallel_extract_citations_ids_keys(doc_id):
    client = MongoClient(maxPoolSize=None)
    articles = client['ami']['articles-issues']
    standardizer = client['citations']['standardized']

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
