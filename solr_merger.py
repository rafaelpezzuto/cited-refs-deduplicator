import logging
import SolrAPI


from pymongo import MongoClient


SOLR_URL='http://localhost:8983/solr/articles'
DEDUPLICATED_CIT_DB=MongoClient()['citations']['deduplicated']


def merge_citations(solr, deduplicated_citations):
    logging.info('Merging Solr documents...')

    for i in deduplicated_citations:
        key = i['key']
        cit_full_ids = i['cit_full_ids']
        citing_docs = i['citing_docs']

        logging.info('Merging data from %s,%s,%s' % (key, cit_full_ids, citing_docs))

        query = 'id:(%s)' % ' OR '.join(cit_full_ids)

        response = solr.select({'q': query, 'rows': 10000})
        dic = eval(response)

        ids_to_remove = set()
        merged_citation = {}

        if len(dic['response']['docs']) > 1:
            
            merged_citation.update(dic['response']['docs'][0])

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
                logging.info('Adding id %s' % merged_citation['id'])
                solr_doc = {
                    'add': {
                        'doc': merged_citation,
                    }
                }
                logging.warning('Adding id %s' % merged_citation['id'])
                solr.update(str(solr_doc).encode('utf-8'), {'content-type': 'application/json'})

            for i in ids_to_remove:
                logging.warning('Removing id %s' % i)
                solr.delete('id:{}'.format(i))

            query = 'id:(%s)' % ' OR '.join(citing_docs)
            response = solr.select({'q': query})
            dic = eval(response)

            for d in dic['response']['docs']:
                logging.warning('Updating id %s' % d['id'])
                updated_doc = {}
                updated_doc['entity'] = 'document'
                updated_doc['id'] = d['id']
                updated_doc['citation_fk'] = {'remove': list(ids_to_remove), 'add': merged_citation['id']}

                solr_doc = {
                    'add': {
                        'doc': updated_doc
                    }
                }

                solr.update(str(solr_doc).encode('utf-8'), headers={'content-type': 'application/json'})


def get_deduplicated_citations():
    deduplicated_citations = []

    logging.warning('Collecting deduplicated citations...')
    for i in DEDUPLICATED_CIT_DB.find():
        if len(i['cit_full_ids']) > 1:
            dc = {
                'key': i['_id'],
                'cit_full_ids': i['cit_full_ids'],
                'citing_docs': i['citing_docs']}
            deduplicated_citations.append(dc)
    
    logging.warning('There are %d deduplicated citations' % len(deduplicated_citations))

    return deduplicated_citations


def main():
    logging.basicConfig(level=logging.WARN)

    solr = SolrAPI.Solr(SOLR_URL)
    
    dup_cits = get_deduplicated_citations()

    merge_citations(solr, dup_cits)
   
    solr.commit()


if __name__ == "__main__":
    main()
