import argparse
import os
import textwrap
import time

from hashlib import sha3_224
from multiprocessing import Pool
from pymongo import MongoClient, UpdateOne
from utils.field_cleaner import get_cleaned_default, get_cleaned_publication_date, get_cleaned_first_author_name, get_cleaned_journal_title
from xylose.scielodocument import Article, Citation


MONGO_STDCITS_DB = os.environ.get('MONGO_STDCITS_DB', 'citations')
MONGO_STDCITS_COLLECTION = os.environ.get('MONGO_STDCITS_COLLECTION', 'standardized')
MONGO_COLLECTION_DEDUP_PREFIX = os.environ.get('MONGO_DEDUP_COLLECTION_PREFIX', 'dedup-')
MONGO_ARTICLES_ISSUES_DB = os.environ.get('MONGO_ARTICLES_ISSUES_DB', 'ami')
MONGO_ARTICLES_ISSUES_COLLECTION = os.environ.get('MONGO_ARTICLES_ISSUES_COLLECTION', 'articles-issues')

ARTICLE_KEYS = ['cleaned_publication_date',
                'cleaned_first_author',
                'cleaned_title',
                'cleaned_journal_title']
BOOK_KEYS = ['cleaned_publication_date',
             'cleaned_first_author',
             'cleaned_source',
             'cleaned_publisher',
             'cleaned_publisher_address']

CHUNKS_INTEVAL = 2000


def _extract_citation_fields_by_list(citation: Citation, fields):
    data = {}

    for f in fields:
        cleaned_v = get_cleaned_default(getattr(citation, f))
        if cleaned_v:
            data['cleaned_' + f] = cleaned_v

    return data


def _extract_citation_authors(citation: Citation):
    data = {}

    if citation.publication_type == 'article' or not citation.chapter_title:
        cleaned_first_author = get_cleaned_first_author_name(citation.first_author)
        if cleaned_first_author:
            data['cleaned_first_author'] = cleaned_first_author
    else:
        if citation.analytic_authors:
            cleaned_chapter_first_author = get_cleaned_first_author_name(citation.analytic_authors[0])
            if cleaned_chapter_first_author:
                data['cleaned_chapter_first_author'] = cleaned_chapter_first_author

            if citation.monographic_authors:
                cleaned_first_author = get_cleaned_first_author_name(citation.monographic_authors[0])
                if cleaned_first_author:
                    data['cleaned_first_author'] = cleaned_first_author

    return data


def extract_citation_data(citation: Citation, cit_standardized_data=None):
    data = {}

    data.update(_extract_citation_authors(citation))

    cleaned_publication_date = get_cleaned_publication_date(citation.publication_date)
    if cleaned_publication_date:
        data['cleaned_publication_date'] = cleaned_publication_date

    if citation.publication_type == 'article':
        data.update(_extract_citation_fields_by_list(citation, ['issue', 'start_page', 'volume']))

        cleaned_journal_title = ''
        if cit_standardized_data:
            cleaned_journal_title = cit_standardized_data['official-journal-title'][0].lower()
            if cleaned_journal_title:
                data['cleaned_journal_title'] = cleaned_journal_title

        if not cleaned_journal_title:
            cleaned_journal_title = get_cleaned_journal_title(citation.source)
            if cleaned_journal_title:
                data['cleaned_journal_title'] = cleaned_journal_title

        cleaned_title = get_cleaned_default(citation.title())
        if cleaned_title:
            data['cleaned_title'] = cleaned_title

    elif citation.publication_type == 'book':
        data.update(_extract_citation_fields_by_list(citation, ['source', 'publisher', 'publisher_address']))

        cleaned_chapter_title = get_cleaned_default(citation.chapter_title)
        if cleaned_chapter_title:
            data['cleaned_chapter_title'] = cleaned_chapter_title

    return data


def mount_citation_id(citation: Citation, collection_acronym):
    cit_id = citation.data['v880'][0]['_']
    cit_full_id = '{0}-{1}'.format(cit_id, collection_acronym)
    return cit_full_id


def hash_keys(cit_data, keys):
    data = []
    for k in keys:
        if k in cit_data:
            if cit_data[k]:
                data.append(k + cit_data[k])
            else:
                return
        else:
            return

    if data:
        return sha3_224(''.join(data).encode()).hexdigest()


def extract_citations_ids_keys(document: Article, standardizer):
    citations_ids_keys = []

    if document.citations:
        for cit in [c for c in document.citations if c.publication_type in {'article', 'book'}]:
            cit_full_id = mount_citation_id(cit, document.collection_acronym)

            if cit.publication_type == 'article':
                cit_standardized_data = standardizer.find_one({'_id': cit_full_id, 'status': {'$gt': 0}})
                cit_data = extract_citation_data(cit, cit_standardized_data)

                for extra_key in ['volume', 'start_page', 'issue']:
                    keys_i = ARTICLE_KEYS + ['cleaned_' + extra_key]

                    article_hash_i = hash_keys(cit_data, keys_i)
                    if article_hash_i:
                        citations_ids_keys.append((cit_full_id,
                                                   {k:cit_data[k] for k in keys_i if k in cit_data},
                                                   article_hash_i,
                                                   'article-' + extra_key))

            else:
                cit_data = extract_citation_data(cit)

                book_hash = hash_keys(cit_data, BOOK_KEYS)
                if book_hash:
                    citations_ids_keys.append((cit_full_id,
                                               {k:cit_data[k] for k in BOOK_KEYS if k in cit_data},
                                               book_hash,
                                               'book'))

                    chapter_keys = BOOK_KEYS + ['cleaned_chapter_title', 'cleaned_chapter_first_author']

                    chapter_hash = hash_keys(cit_data, chapter_keys)
                    if chapter_hash:
                        citations_ids_keys.append((cit_full_id,
                                                   {k:cit_data[k] for k in chapter_keys if k in cit_data},
                                                   chapter_hash,
                                                   'chapter'))

    return citations_ids_keys


def convert_to_mongodoc(data):
    mgdocs = {'article-issue': {}, 'article-start_page': {}, 'article-volume': {},'book': {}, 'chapter': {}}

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
    mongo_data = convert_to_mongodoc(data)

    for k, v in mongo_data.items():
        client = MongoClient(MONGO_URI)
        writer = client[MONGO_STDCITS_DB][MONGO_COLLECTION_DEDUP_PREFIX + k]

        operations = []
        for cit_sha3_256 in v:
            new_doc = v[cit_sha3_256]
            operations.append(UpdateOne(
                filter={'_id': str(cit_sha3_256)},
                update={
                    '$set': {'cit_keys': new_doc['cit_keys']},
                    '$addToSet': {
                        'cit_full_ids': {'$each': new_doc['cit_full_ids']},
                        'citing_docs': {'$each': new_doc['citing_docs']},
                    }
                },
                upsert=True
            ))

            if len(operations) == 1000:
                writer.bulk_write(operations)
                operations = []

        if len(operations) > 0:
            writer.bulk_write(operations)

        client.close()


def parallel_extract_citations_ids_keys(doc_id):
    client = MongoClient(MONGO_URI, maxPoolSize=None)
    articles = client[MONGO_ARTICLES_ISSUES_DB][MONGO_ARTICLES_ISSUES_COLLECTION]
    standardizer = client[MONGO_STDCITS_DB][MONGO_STDCITS_COLLECTION]

    raw = articles.find_one({'_id': doc_id})
    doc = Article(raw)

    citations_keys = extract_citations_ids_keys(doc, standardizer)
    if citations_keys:
        return '-'.join([doc.publisher_id, doc.collection_acronym]), citations_keys


usage = """\
    Gera chaves de de-deduplicaçao que contem IDs de referencias citadas a serem mescladas no Solr.
    """

parser = argparse.ArgumentParser(textwrap.dedent(usage))

parser.add_argument(
    '--mongo_uri',
    default=None,
    dest='mongo_uri',
    help='String de conexao a base Mongo no formato mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[defaultauthdb][?options]]'
)

params = parser.parse_args()
MONGO_URI = params.mongo_uri

print('[1] Getting documents\' ids...')
start = time.time()

main_client = MongoClient(MONGO_URI, maxPoolSize=None)
docs_ids = [x['_id'] for x in main_client[MONGO_ARTICLES_ISSUES_DB][MONGO_ARTICLES_ISSUES_COLLECTION].find({}, {'_id': 1})]
total_docs = len(docs_ids)
main_client.close()

end = time.time()
print('\tDone after %.2f seconds' % (end - start))

start = time.time()

print('[2] Generating keys...')
chunks = range(0, total_docs, CHUNKS_INTEVAL)
for slice_start in chunks:
    slice_end = slice_start + CHUNKS_INTEVAL
    if slice_end > total_docs:
        slice_end = total_docs

    print('\t%d to %d' % (slice_start, slice_end))
    with Pool(4) as p:
        results = p.map(parallel_extract_citations_ids_keys, docs_ids[slice_start:slice_end])

    save_data_to_mongo(results)
end = time.time()
print('\tDone after %.2f seconds' % (end - start))
