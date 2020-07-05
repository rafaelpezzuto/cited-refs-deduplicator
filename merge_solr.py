import argparse
import datetime
import logging
import os
import SolrAPI
import textwrap

from pymongo import MongoClient


MONGO_COLLECTION_DEDUP_PREFIX = os.environ.get('MONGO_DEDUP_COLLECTION_PREFIX', 'dedup')
MONGO_DB_DEDUP = os.environ.get('MONGO_DEDUP_DB', 'citations')
MONGO_ARTICLES_ISSUES_DB = os.environ.get('MONGO_ARTICLES_ISSUES_DB', 'ami')
MONGO_ARTICLES_ISSUES_COLLECTION = os.environ.get('MONGO_ARTICLES_ISSUES_COLLECTION', 'articles-issues')
SOLR_URL = os.environ.get('SOLR_URL', 'http://localhost:8983/solr/articles')
SOLR_ROWS_LIMIT = 10000


def get_ids_for_merging(client: MongoClient):
    logging.info('Getting IDs for merging...')

    ids_for_merging = {'book': [],
                       'chapter': [],
                       'article-issue': [],
                       'article-start_page': [],
                       'article-volume': []}

    for col in ['book',
                'chapter',
                'article-issue',
                'article-start_page',
                'article-volume']:

        for j in client[MONGO_DB_DEDUP][MONGO_COLLECTION_DEDUP_PREFIX + '-' + col].find(
                {'cit_full_ids.1': {'$exists': True}}):

            item = {
                '_id': j['_id'],
                'cit_full_ids': j['cit_full_ids'],
                'citing_docs': j['citing_docs']
            }

            if col == 'article-issue':
                item.update({'cit_issue': j['cit_keys']['cleaned_issue']})

            if col == 'article-start_page':
                item.update({'cit_start_page': j['cit_keys']['cleaned_start_page']})

            if col == 'article-volume':
                item.update({'cit_volume': j['cit_keys']['cleaned_volume']})

            ids_for_merging[col].append(item)

        logging.info('%d' % len(ids_for_merging[col]))
    return ids_for_merging


def merge_citations(solr, deduplicated_citations, base):
    logging.info('Merging Solr documents...')
    counter = 1

    for dc in deduplicated_citations:
        print('\r%d' % counter, end='')
        counter += 1
        key = dc['_id']
        cit_full_ids = dc['cit_full_ids']
        citing_docs = dc['citing_docs']

        logging.info('Merging data for ID %s (CIT %s) (ART %s)' % (key, '#'.join(cit_full_ids), '#'.join(citing_docs)))

        query = 'id:(%s)' % ' OR '.join(cit_full_ids)

        response = solr.select({'q': query, 'rows': SOLR_ROWS_LIMIT})
        dic = eval(response)

        ids_to_remove = set()
        merged_citation = {}

        if len(dic['response']['docs']) > 1:

            merged_citation.update(dic['response']['docs'][0])

            if base == 'articles-start_page':
                merged_citation['start_page'] = dc['cit_start_page']
            elif base == 'articles-volume':
                merged_citation['volume'] = dc['cit_volume']
            elif base == 'articles-issue':
                merged_citation['issue'] = dc['cit_issue']

            for d in dic['response']['docs'][1:]:
                raw_d = d.copy()
                merged_citation['document_fk'].extend(raw_d['document_fk'])
                merged_citation['document_fk'] = list(set(merged_citation['document_fk']))
                merged_citation['total_received'] = str(len(merged_citation['document_fk']))

                merged_citation['in'].extend(d['in'])
                merged_citation['in'] = list(set(merged_citation['in']))

                if 'document_fk_au' in raw_d:
                    if 'document_fk_au' not in merged_citation:
                        merged_citation['document_fk_au'] = []
                    merged_citation['document_fk_au'].extend(d['document_fk_au'])
                    merged_citation['document_fk_au'] = list(set(merged_citation['document_fk_au']))

                if 'document_fk_ta' in raw_d:
                    if 'document_fk_ta' not in merged_citation:
                        merged_citation['document_fk_ta'] = []
                    merged_citation['document_fk_ta'].extend(d['document_fk_ta'])
                    merged_citation['document_fk_ta'] = list(set(merged_citation['document_fk_ta']))

                ids_to_remove.add(d['id'])

            if merged_citation:
                logging.debug('Adding id %s' % merged_citation['id'])
                solr_doc = {
                    'add': {
                        'doc': merged_citation,
                    }
                }
                logging.debug('Adding id %s' % merged_citation['id'])
                solr.update(str(solr_doc).encode('utf-8'), {'content-type': 'application/json'})

            for i in ids_to_remove:
                logging.debug('Removing id %s' % i)
                solr.delete('id:{}'.format(i))

            query = 'id:(%s)' % ' OR '.join(citing_docs)
            response = solr.select({'q': query})
            dic = eval(response)

            docs_for_updating = []
            for d in dic['response']['docs']:
                logging.debug('Updating id %s' % d['id'])
                updated_doc = {}
                updated_doc['entity'] = 'document'
                updated_doc['id'] = d['id']
                updated_doc['citation_fk'] = {'remove': list(ids_to_remove), 'add': merged_citation['id']}

                solr_doc = {
                    'add': {
                        'doc': updated_doc
                    }
                }

                docs_for_updating.append(str(solr_doc).encode('utf-8'))

            solr.update(str(docs_for_updating), headers={'content-type': 'application/json'})

    solr.commit()


def main():
    usage = """\
        Mescla documentos Solr do tipo citation.
        """

    parser = argparse.ArgumentParser(textwrap.dedent(usage))

    parser.add_argument(
        '--mongo_uri',
        default=None,
        dest='mongo_uri',
        help='String de conexao a base Mongo no formato mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[defaultauthdb][?options]]'
    )

    params = parser.parse_args()

    logging.basicConfig(filename='merge_solr-' + datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S') + '.log', level=logging.DEBUG)

    client = MongoClient(params.mongo_uri)
    ids_to_merge = get_ids_for_merging(client)

    solr = SolrAPI.Solr(SOLR_URL)

    for k, v in ids_to_merge.items():
        merge_citations(solr, v, k)


    # for base in ['dt3-fp', 'dt3-vol', 'dt3-issue']:
    #     dup_cits = get_deduplicated_citations(base)
    #     merge_citations(solr, dup_cits, base)
    #     print()
    #     solr.commit()


if __name__ == "__main__":
    main()
