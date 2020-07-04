import time

from hashlib import sha3_256
from multiprocessing import Pool
from pymongo import MongoClient
from utils.field_cleaner import get_cleaned_publication_year, get_cleaned_first_author_name, get_cleaned_publisher, get_cleaned_publisher_address
from utils.string_processor import preprocess_author_name
from xylose.scielodocument import Article, Citation


def _extract_citation_key(citation: Citation):
    data = {
        'cleaned_title': '',
        'cleaned_publication_year': '',
        'cleaned_first_author': '',
        'cleaned_publisher': '',
        'cleaned_publisher_address': ''
    }

    cleaned_publication_year = get_cleaned_publication_year(citation.publication_date)
    if cleaned_publication_year:
        data['cleaned_publication_year'] = cleaned_publication_year

    cleaned_title = preprocess_author_name(citation.source).lower()
    if cleaned_title:
        data['cleaned_title'] = cleaned_title

    cleaned_publisher = get_cleaned_publisher(citation.publisher).lower()
    if cleaned_publisher:
        data['cleaned_publisher'] = cleaned_publisher

    cleaned_publisher_address = get_cleaned_publisher_address(citation.publisher).lower()
    if cleaned_publisher_address:
        data['cleaned_publisher_address'] = cleaned_publisher_address

    if citation.chapter_title:
        cleaned_chapter_title = preprocess_author_name(citation.chapter_title).lower()
        if cleaned_chapter_title:
            data['cleaned_chapter_title'] = cleaned_chapter_title

        if citation.analytic_authors:
            cleaned_chapter_first_author = get_cleaned_first_author_name(citation.analytic_authors[0])
            if cleaned_chapter_first_author:
                data['cleaned_chapter_first_author'] = cleaned_chapter_first_author

        if citation.monographic_authors:
            cleaned_first_author = get_cleaned_first_author_name(citation.monographic_authors[0])
            if cleaned_first_author:
                data['cleaned_first_author'] = cleaned_first_author
    else:
        cleaned_first_author = get_cleaned_first_author_name(citation.first_author)
        if cleaned_first_author:
            data['cleaned_first_author'] = cleaned_first_author

    return data


def _mount_citation_id(citation: Citation, collection_acronym):
    cit_id = citation.data['v880'][0]['_']
    cit_full_id = '{0}-{1}'.format(cit_id, collection_acronym)
    return cit_full_id


def _generate_hashes(cit_keys):
    t = cit_keys.get('cleaned_title')
    py = cit_keys.get('cleaned_publication_year')
    fau = cit_keys.get('cleaned_first_author')
    pub = cit_keys.get('cleaned_publisher')
    puba = cit_keys.get('cleaned_publisher_address')

    ct = cit_keys.get('cleaned_chapter_title')
    ca = cit_keys.get('cleaned_chapter_first_author')

    t_py_j_fau_pub_puba = ''
    t_py_j_fau_pub_puba_ct_ca = ''

    if t and py and fau and pub and puba:
        t_py_j_fau_pub_puba = sha3_256(''.join(['TITLE' + t, 'YEAR' + py, 'FIRST_AUTHOR' + fau, 'PUBLISHER' + pub, 'PUBLISHER_ADDRESS' + puba]).encode()).hexdigest()
        if ct and ca:
            t_py_j_fau_pub_puba_ct_ca = sha3_256(''.join(['TITLE' + t, 'YEAR' + py, 'FIRST_AUTHOR' + fau, 'PUBLISHER' + pub, 'PUBLISHER_ADDRESS' + puba, 'CHAPTER_TITLE' + ct, 'CHAPTER_FIRST_AUTHOR' + ca]).encode()).hexdigest()

    return t_py_j_fau_pub_puba, t_py_j_fau_pub_puba_ct_ca


def extract_citations_ids_keys(document: Article):
    citations_ids_keys = []

    if document.citations:
        for cit in [c for c in document.citations if c.publication_type == 'book']:
            cit_full_id = _mount_citation_id(cit, document.collection_acronym)

            cit_keys = _extract_citation_key(cit)
            hbook, hchapter = _generate_hashes(cit_keys)

            if hbook:
                citations_ids_keys.append((cit_full_id, cit_keys, hbook, 'book'))
            if hchapter:
                citations_ids_keys.append((cit_full_id, cit_keys, hchapter, 'chapter'))

    return citations_ids_keys


def convert_to_mongodoc(data):
    mgdocs = {'book': {}, 'chapter': {}}
    for doc_id, citations_data in [d for d in data if d]:
        for cit in citations_data:
            cit_full_id = cit[0]
            cit_keys = cit[1]
            cit_sha3_256 = cit[2]
            cit_hash_mode = cit[3]

            if cit_sha3_256 not in mgdocs:
                mgdocs[cit_hash_mode][cit_sha3_256] = {'cit_full_ids': [], 'citing_docs': [], 'cit_keys': cit_keys}

            mgdocs[cit_hash_mode][cit_sha3_256]['cit_full_ids'].append(cit_full_id)
            mgdocs[cit_hash_mode][cit_sha3_256]['citing_docs'].append(doc_id)

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
                    }
                },
                upsert=True)

        mdb.close()


def parallel_extract_citations_ids_keys(doc_id):
    client = MongoClient(maxPoolSize=None)
    articles = client['ami']['articles-issues']

    raw = articles.find_one({'_id': doc_id})
    doc = Article(raw)

    citations_keys = extract_citations_ids_keys(doc)
    if citations_keys:
        return (('-'.join([doc.publisher_id, doc.collection_acronym]), citations_keys))


print('[1] Getting documents\' ids...')
start = time.time()

# TODO: remove hardcoded MongoConnection
main_client = MongoClient(maxPoolSize=None)
docs_ids = [x['_id'] for x in main_client['ami']['articles-issues'].find({}, {'_id': 1}).limit(10000)]
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
