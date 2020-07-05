import datetime
import logging
import SolrAPI

from pymongo import MongoClient


SOLR_URL='http://localhost:8983/solr/articles'
MONGO_DB=MongoClient()['citations']


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

        response = solr.select({'q': query, 'rows': 10000})
        dic = eval(response)

        ids_to_remove = set()
        merged_citation = {}

        if len(dic['response']['docs']) > 1:

            merged_citation.update(dic['response']['docs'][0])
            if base == 'dt3-fp':
                merged_citation['start_page'] = dc['cit_first_page']
            elif base == 'dt3-vol':
                merged_citation['volume'] = dc['cit_vol']
            elif base == 'dt3-issue':
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

            docs_to_be_updated = []
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

                docs_to_be_updated.append(str(solr_doc).encode('utf-8'))

            solr.update(str(docs_to_be_updated), headers={'content-type': 'application/json'})


def get_deduplicated_citations(db):
    deduplicated_citations = []

    logging.info('Collecting deduplicated pairs from %s...' % db)
    for x, i in enumerate(MONGO_DB[db].find()):
        print('\r%s %d' % (db, x + 1), end='')
        if len(i['cit_full_ids']) > 1:
            dc = {
                '_id': i['_id'],
                'cit_full_ids': i['cit_full_ids'],
                'citing_docs': i['citing_docs']}

            if i == 'dt3-fp':
                dc.update({'cit_first_page': i['cit_first_page']})
            if i == 'dt3-vol':
                dc.update({'cit_vol': i['cit_vol']})
            if i == 'dt3-issue':
                dc.update({'cit_issue': i['cit_issue']})

            deduplicated_citations.append(dc)
    print()
    logging.info('There are %d citations to be deduplicated' % len(deduplicated_citations))
    return deduplicated_citations


def main():
    logging.basicConfig(filename='solr-merger-' + datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S') + '.log', level=logging.DEBUG)

    solr = SolrAPI.Solr(SOLR_URL)

    for base in ['dt3-fp', 'dt3-vol', 'dt3-issue']:
        dup_cits = get_deduplicated_citations(base)
        merge_citations(solr, dup_cits, base)
        print()
        solr.commit()


if __name__ == "__main__":
    main()
