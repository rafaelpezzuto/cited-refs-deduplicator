from utils.string_processor import preprocess_author_name


def get_cleaned_first_author_name(first_author: dict):
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
    return ''


def get_cleaned_first_page(first_page: str):
    if first_page:
        return preprocess_author_name(first_page).lower()
    return ''


def get_cleaned_issue(issue: str):
    if issue:
        return preprocess_author_name(issue).lower()
    return ''


def get_cleaned_last_page(first_page: str, last_page: str):
    cleaned_first_page = preprocess_author_name(first_page).lower()
    cleaned_last_page = preprocess_author_name(last_page).lower()

    diff_len_pages = len(cleaned_first_page) - len(cleaned_last_page)

    if diff_len_pages > 0:
        return ''.join([cleaned_first_page[:diff_len_pages]] + [cleaned_last_page])
    else:
        return preprocess_author_name(last_page).lower()


def get_cleaned_publication_year(publication_date: str):
    if publication_date:
        cleaned_publication_year = preprocess_author_name(publication_date)
        if cleaned_publication_year:
            if len(cleaned_publication_year) > 4:
                cleaned_publication_year = cleaned_publication_year[:4]
            return cleaned_publication_year


def get_cleaned_publisher(publisher: str):
    if publisher:
        return preprocess_author_name(publisher)
    return ''


def get_cleaned_publisher_address(publisher_address: str):
    if publisher_address:
        return preprocess_author_name(publisher_address)
    return ''


def get_cleaned_volume(volume: str):
    if volume:
        return preprocess_author_name(volume).lower()
