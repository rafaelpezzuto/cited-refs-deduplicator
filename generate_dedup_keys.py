import argparse
import os
import textwrap
import time

from datetime import datetime
from hashlib import sha3_224
from multiprocessing import Pool
from pymongo import MongoClient, UpdateOne
from utils.field_cleaner import get_cleaned_default, get_cleaned_publication_date, get_cleaned_first_author_name, get_cleaned_journal_title
from xylose.scielodocument import Article, Citation


MONGO_URI = os.environ.get('mongodb://localhost:27017/', None)

MONGO_DB_SEARCH_SCIELO = os.environ.get('MONGO_DB_SEARCH_SCIELO', 'citations')

MONGO_COLLECTION_STANDARDIZED_CITATIONS = os.environ.get('MONGO_COLLECTION_STANDARDIZED_CITATIONS', 'standardized')
MONGO_COLLECTION_DEDUPLICATED_CITATIONS_PREFIX = os.environ.get('MONGO_DEDUP_COLLECTION_PREFIX', 'dedup-')

MONGO_DB_ARTICLES = os.environ.get('MONGO_DB_ARTICLES', 'ami')
MONGO_COLLECTION_ARTICLES = os.environ.get('MONGO_COLLECTION_ARTICLES', 'articles-issues')

ARTICLE_KEYS = ['cleaned_publication_date',
                'cleaned_first_author',
                'cleaned_title',
                'cleaned_journal_title']

BOOK_KEYS = ['cleaned_publication_date',
             'cleaned_first_author',
             'cleaned_source',
             'cleaned_publisher',
             'cleaned_publisher_address']

chunk_size = 2000

citation_types = set()


def _extract_citation_fields_by_list(citation: Citation, fields):
    """
    Extrai de uma citaçao os campos indicados na variavel fields.

    :param citation: Citaçao da qual serao extraidos os campos
    :param fields: Campos a serem extraidos
    :return: Dicionario composto pelos pares campo: valor do campo
    """
    data = {}

    for f in fields:
        cleaned_v = get_cleaned_default(getattr(citation, f))
        if cleaned_v:
            data['cleaned_' + f] = cleaned_v

    return data


def _extract_citation_authors(citation: Citation):
    """
    Extrai o primeiro autor de uma citaçao.
    Caso citaçao seja capitulo de livro, extrai o primeiro autor do livro e o primeiro autor do capitulo.
    Caso citaçao seja livro ou artigo, extrai o primeiro autor.

    :param citation: Citaçao da qual o primeiro autor sera extraido
    :return: Dicionario composto pelos pares cleaned_first_author: valor e cleaned_chapter_first_author: valor
    """
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
    """
    Extrai os dados de uma citaçao.

    :param citation: Citaçao da qual os dados serao extraidos
    :param cit_standardized_data: Caso seja artigo, usa o padronizador de titulo de periodico
    :return: Dicionario composto pelos pares de nomes dos ampos limpos das citaçoes e respectivos valores
    """
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
    """
    Monta o id completo de uma citaçao.

    :param citation: Citaçao da qual o id completo sera montado
    :param collection_acronym: Acronimo da coleçao SciELO na qual a citaçao foi referida
    :return: ID completo da citaçao formada pelo PID do documento citante, numero da citaçao e coleçao citante
    """
    cit_id = citation.data['v880'][0]['_']
    cit_full_id = '{0}-{1}'.format(cit_id, collection_acronym)
    return cit_full_id


def hash_keys(cit_data, keys):
    """
    Cria um codigo hash dos dados de uma citaçao, com base na lista de keys.

    :param cit_data: Dicionario de pares de nome de campo e valor de campo de citaçao
    :param keys: Nomes dos campos a serem considerados para formar o codigo hash
    :return: Codigo hash SHA3_256 para os dados da citaçao
    """
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
    """
    Extrai as quadras (id de citaçao, pares de campos de citaçao, hash da citaçao, base) para todos as citaçoes.
    Sao contemplados livros, capitulos de livros e artigos.

    :param document: Documento do qual a lista de citaçoes sera convertida para hash
    :param standardizer: Normalizador de titulo de periodico citado
    :return: Quadra composta por id de citacao, dicionario de nomes de campos e valores, hash de citaçao e base
    """
    citations_ids_keys = []

    if document.citations:
        for cit in [c for c in document.citations if c.publication_type in citation_types]:
            cit_full_id = mount_citation_id(cit, document.collection_acronym)

            if cit.publication_type == 'article':
                cit_standardized_data = standardizer.find_one({'_id': cit_full_id, 'status': {'$gt': 0}})
                cit_data = extract_citation_data(cit, cit_standardized_data)

                for extra_key in ['volume', 'start_page', 'issue']:
                    keys_i = ARTICLE_KEYS + ['cleaned_' + extra_key]

                    article_hash_i = hash_keys(cit_data, keys_i)
                    if article_hash_i:
                        citations_ids_keys.append((cit_full_id,
                                                   {k: cit_data[k] for k in keys_i if k in cit_data},
                                                   article_hash_i,
                                                   'article-' + extra_key))

            else:
                cit_data = extract_citation_data(cit)

                book_hash = hash_keys(cit_data, BOOK_KEYS)
                if book_hash:
                    citations_ids_keys.append((cit_full_id,
                                               {k: cit_data[k] for k in BOOK_KEYS if k in cit_data},
                                               book_hash,
                                               'book'))

                    chapter_keys = BOOK_KEYS + ['cleaned_chapter_title', 'cleaned_chapter_first_author']

                    chapter_hash = hash_keys(cit_data, chapter_keys)
                    if chapter_hash:
                        citations_ids_keys.append((cit_full_id,
                                                   {k: cit_data[k] for k in chapter_keys if k in cit_data},
                                                   chapter_hash,
                                                   'chapter'))

    return citations_ids_keys


def convert_to_mongodoc(data):
    """
    Converte dados de citaçao para registro em formato Mongo.

    :param data: Dados a serem convertidos (lista de quadras no formato: id de citacao, dados de citaçao, hash, base)
    :return: Dados convertidos
    """
    mgdocs = {'article-issue': {}, 'article-start_page': {}, 'article-volume': {}, 'book': {}, 'chapter': {}}

    for doc_id, citations_data in [d for d in data if d]:
        for cit in citations_data:
            cit_full_id = cit[0]
            cit_keys = cit[1]
            cit_sha3_256 = cit[2]
            cit_hash_mode = cit[3]

            if cit_sha3_256 not in mgdocs[cit_hash_mode]:
                mgdocs[cit_hash_mode][cit_sha3_256] = {'cit_full_ids': [], 'citing_docs': [], 'cit_keys': cit_keys}

            mgdocs[cit_hash_mode][cit_sha3_256]['cit_full_ids'].append(cit_full_id)
            mgdocs[cit_hash_mode][cit_sha3_256]['citing_docs'].append(doc_id)
            mgdocs[cit_hash_mode][cit_sha3_256]['update_date'] = datetime.now().strftime('%Y-%m-%d')

    return mgdocs


def save_data_to_mongo(data):
    """
    Persiste na base Mongo os dados das chaves de de-duplicaçao.

    :param data: Dados a serem persistidos
    :param mongo_uri: String de conexao a base Mongo na qual os dados serao persistidos
    """
    mongo_data = convert_to_mongodoc(data)

    for k, v in mongo_data.items():
        client = MongoClient(MONGO_URI)
        writer = client[MONGO_DB_SEARCH_SCIELO][MONGO_COLLECTION_DEDUPLICATED_CITATIONS_PREFIX + k]

        operations = []
        for cit_sha3_256 in v:
            new_doc = v[cit_sha3_256]
            operations.append(UpdateOne(
                filter={'_id': str(cit_sha3_256)},
                update={
                    '$set': {
                        'cit_keys': new_doc['cit_keys'],
                        'update_date': new_doc['update_date']
                    },
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
    articles = client[MONGO_DB_ARTICLES][MONGO_COLLECTION_ARTICLES]
    standardizer = client[MONGO_DB_SEARCH_SCIELO][MONGO_COLLECTION_STANDARDIZED_CITATIONS]

    raw = articles.find_one({'_id': doc_id})
    doc = Article(raw)

    citations_keys = extract_citations_ids_keys(doc, standardizer)
    if citations_keys:
        return '-'.join([doc.publisher_id, doc.collection_acronym]), citations_keys


def main():
    usage = "Gera chaves de de-duplicaçao de artigos, livros e capitulos citados."

    parser = argparse.ArgumentParser(textwrap.dedent(usage))

    parser.add_argument(
        '-f', '--from_date',
        type=lambda x: datetime.strftime(x, '%Y-%m-%d'),
        nargs='?',
        help='Obtem apenas os PIDs de artigos publicados a partir da data especificada (use o formato YYYY-MM-DD)'
    )

    parser.add_argument(
        '-b', '--book',
        action='store_true',
        default=None,
        help='Obtem chaves para livros ou capitulos de livros citados'
    )

    parser.add_argument(
        '-a', '--article',
        action='store_true',
        default=None,
        help='Obtem chaves para artigos citados'
    )

    parser.add_argument(
        '-c', '--chunk_size',
        help='Tamanho de cada slice Mongo'
    )

    args = parser.parse_args()

    global citation_types
    global chunk_size

    mongo_filter = {}
    if args.from_date:
        mongo_filter.update({'publication_date': args.from_date})

    if args.book:
        citation_types.add('book')
    if args.article:
        citation_types.add('article')

    if args.chunk_size and args.chunk_size.isdigit() and int(args.chunk_size) > 0:
        chunk_size = int(args.chunk_size)

    print('[Settings] citation types: %s, chunk size: %d, mongo filter: %s' % (citation_types, chunk_size, mongo_filter))
    print('[1] Getting documents\' ids...')
    start = time.time()

    main_client = MongoClient(MONGO_URI, maxPoolSize=None)
    docs_ids = [x['_id'] for x in main_client[MONGO_DB_ARTICLES][MONGO_COLLECTION_ARTICLES].find(mongo_filter, {'_id': 1})]
    total_docs = len(docs_ids)
    main_client.close()

    end = time.time()
    print('\tDone after %.2f seconds' % (end - start))

    start = time.time()

    print('[2] Generating keys...')
    chunks = range(0, total_docs, chunk_size)
    for slice_start in chunks:
        slice_end = slice_start + chunk_size
        if slice_end > total_docs:
            slice_end = total_docs

        print('\t%d to %d' % (slice_start, slice_end))
        with Pool(os.cpu_count()) as p:
            results = p.map(parallel_extract_citations_ids_keys, docs_ids[slice_start:slice_end])

        save_data_to_mongo(results)
    end = time.time()
    print('\tDone after %.2f seconds' % (end - start))


if __name__ == '__main__':
    main()
