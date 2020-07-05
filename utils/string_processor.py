import html
import re
import unicodedata


parenthesis_pattern = re.compile(r'[-a-zA-ZÀ-ÖØ-öø-ÿ|0-9]*\([-a-zA-ZÀ-ÖØ-öø-ÿ|\W|0-9]*\)[-a-zA-ZÀ-ÖØ-öø-ÿ|0-9]*', re.UNICODE)
doi_pattern = re.compile(r'\d{2}\.\d+/.*$')
special_chars = ['@', '&']
special_words = ['IMPRESSO', 'ONLINE', 'CDROM', 'PRINT', 'ELECTRONIC']


def remove_invalid_chars(text):
    """
    Remove de text os caracteres que possuem código ASCII < 32 e = 127.
    :param text: texto a ser tratada
    :return: texto com caracteres ASCII < 32 e = 127 removidos
    """
    vchars = []
    for t in text:
        if ord(t) == 11:
            vchars.append(' ')
        elif ord(t) >= 32 and ord(t) != 127:
            vchars.append(t)
    return ''.join(vchars)


def remove_accents(text):
    """
    Transforma caracteres acentuados de text em caracteres sem acento.
    :param text: texto a ser tratado
    :return: texto sem caracteres acentuados
    """
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')


def alpha_num_space(text, include_special_chars=False):
    """
    Mantém em text apenas caracteres alpha, numéricos e espaços.
    Possibilita manter em text caracteres especiais na lista special_chars
    :param text: texto a ser tratado
    :param include_special_chars: booleano que indica se os caracteres especiais devem ou não ser mantidos
    :return: texto com apenas caracteres alpha e espaço mantidos (e especiais, caso solicitado)
    """
    new_str = []
    for character in text:
        if character.isalnum() or character.isspace() or (include_special_chars and character in special_chars):
            new_str.append(character)
        else:
            new_str.append(' ')
    return ''.join(new_str)


def remove_double_spaces(text):
    """
    Remove espaços duplos de text
    :param text: texto a ser tratado
    :return: texto sem espaços duplos
    """
    while '  ' in text:
        text = text.replace('  ', ' ')
    return text.strip()


def preprocess_default(text):
    """
    Aplica:
        1. Remoçao de acentos
        2. Manutençao d alpha e espaco
        3. Remoçao de espaços duplos
    Procedimento que faz tratamento padrao de limpeza
    :param text: string a ser tratada
    :return: string tratada
    """
    return remove_double_spaces(alpha_num_space((remove_accents(text))))


def preprocess_author_name(text):
    """
    Procedimento que trata nome de autor.
    Aplica:
        1. Remoção de acentos
        2. Manutenção de alpha e espaço
        3. Remoção de espaços duplos
    :param text: nome do autor a ser tratado
    :return: nome tratado do autor
    """
    return remove_double_spaces(alpha_num_space(remove_accents(text)))


def preprocess_doi(text):
    """
    Procedimento que trata DOI.

    :param text: caracteres que representam um código DOI
    :return: código DOI tratado
    """
    doi = doi_pattern.findall(text)
    if len(doi) == 1:
        return doi[0]


def preprocess_journal_title(text, use_remove_invalid_chars=False):
    """
    Procedimento para tratar título de periódico.
    Aplica:
        1. Tratamento de caracteres HTML
        2. Remoção de caracteres inválidos
        3. Remoção de dados entre parenteses
        4. Remoção de acentos, inclusive caracteres especiais
        5. Manutenção de apenas caracteres alpha, numérico e espaço
        6. Remoção de espaços duplos
        7. Remove palavras especiais
        8. Transforma caracteres para caixa alta
    :param text: título do periódico a ser tratado
    :param use_remove_invalid_chars: boolenano que indica se deve ou não ser aplicada remoção de caracteres inválidos
    :return: título tratado do periódico
    """
    # Trata conteúdo HTML
    text = html.unescape(text)

    # Caso solicitado, remove caracteres inválidos
    if use_remove_invalid_chars:
        text = remove_invalid_chars(text)

    # Remove parenteses e conteúdo interno
    parenthesis_search = re.search(parenthesis_pattern, text)
    while parenthesis_search is not None:
        text = text[:parenthesis_search.start()] + text[parenthesis_search.end():]
        parenthesis_search = re.search(parenthesis_pattern, text)

    # Remove palavras especiais
    for sw in special_words:
        text = text.replace(sw, '')
    return remove_double_spaces(alpha_num_space(remove_accents(text), include_special_chars=True)).lower()
