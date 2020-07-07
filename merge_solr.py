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
SOLR_ROWS_LIMIT = 2000


def dump_deduping_data(data, filename, base):
    with open(base + '-' + filename + '-' + datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S') + '.json', 'w') as f:
        f.write(data)


def get_ids_for_merging(client: MongoClient, base):
    logging.info('Getting IDs for merging...')

    ids_for_merging = []

    for j in client[MONGO_DB_DEDUP][MONGO_COLLECTION_DEDUP_PREFIX + '-' + base].find({'cit_full_ids.1': {'$exists': True}}):

        item = {
            '_id': j['_id'],
            'cit_full_ids': j['cit_full_ids'],
            'citing_docs': j['citing_docs']
        }

        if base == 'article-issue':
            item.update({'cit_issue': j['cit_keys']['cleaned_issue']})

        if base == 'article-start_page':
            item.update({'cit_start_page': j['cit_keys']['cleaned_start_page']})

        if base == 'article-volume':
            item.update({'cit_volume': j['cit_keys']['cleaned_volume']})

        ids_for_merging.append(item)

    logging.info('%d' % len(ids_for_merging))
    return ids_for_merging


def merge_citations(solr, deduplicated_citations, base):
    logging.info('Merging Solr documents...')
    counter = 1

    cits_for_merging = []
    docs_for_updating = []
    cits_for_removing = set()

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

                ids_to_remove.add(raw_d['id'])

            if merged_citation:
                logging.debug('Adding id %s' % merged_citation['id'])
                cits_for_merging.append(merged_citation)

            for i in ids_to_remove:
                logging.debug('Removing id %s' % i)
                cits_for_removing.add(i)

            query = 'id:(%s)' % ' OR '.join(citing_docs)
            response = solr.select({'q': query})
            dic = eval(response)

            for d in dic['response']['docs']:
                logging.debug('Updating id %s' % d['id'])
                updated_doc = {}
                updated_doc['entity'] = 'document'
                updated_doc['id'] = d['id']
                updated_doc['citation_fk'] = {'remove': list(ids_to_remove), 'add': merged_citation['id']}

                docs_for_updating.append(updated_doc)

        if len(cits_for_merging) == 1000:
            dump_deduping_data(str(cits_for_merging), 'cits_for_merging', base)
            solr.update(str(cits_for_merging).encode('utf-8'), headers={'content-type': 'application/json'})
            cits_for_merging = []

            dump_deduping_data(str(docs_for_updating), 'docs_for_updating', base)
            solr.update(str(docs_for_updating).encode('utf-8'), headers={'content-type': 'application/json'})
            docs_for_updating = []

            dump_deduping_data(str({'delete': {'query': 'id:(' + ' OR '.join(cits_for_removing) + ')'}}), 'cits_for_removing', base)
            solr.delete('id:(' + ' OR '.join(cits_for_removing) + ')')
            cits_for_removing = set()

    if len(cits_for_merging) > 0:
        dump_deduping_data(str(cits_for_merging), 'cits_for_merging', base)
        solr.update(str(cits_for_merging).encode('utf-8'), headers={'content-type': 'application/json'})

    if len(docs_for_updating) > 0:
        dump_deduping_data(str(docs_for_updating), 'docs_for_updating', base)
        solr.update(str(docs_for_updating).encode('utf-8'), headers={'content-type': 'application/json'})

    if len(cits_for_removing) > 0:
        dump_deduping_data(str({'delete': {'query': 'id:(' + ' OR '.join(cits_for_removing) + ')'}}), 'cits_for_removing', base)
        solr.delete('id:(' + ' OR '.join(cits_for_removing) + ')')

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

    parser.add_argument(
        '--base',
        default=None,
        dest='base'
    )

    params = parser.parse_args()

    logging.basicConfig(filename='merge_solr-' + datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S') + '.log', level=logging.DEBUG)

    client = MongoClient(params.mongo_uri)
    ids_to_merge = get_ids_for_merging(client, params.base)

    solr = SolrAPI.Solr(SOLR_URL, timeout=100)

    merge_citations(solr, ids_to_merge, params.base)


if __name__ == "__main__":
    main()
