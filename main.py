import requests
from bs4 import BeautifulSoup
import csv
import os
import signal
import sys
from urllib.parse import urljoin, urlparse
import re
import string
from collections import defaultdict
import pymorphy3
import json
import math
from collections import Counter


class WebSpider:

    def __init__(self, start_url, min_pages=100, max_depth=3):
        self.start_url = start_url
        self.min_pages = min_pages
        self.max_depth = max_depth
        self.base_domain = urlparse(start_url).netloc
        self.visited_urls = set()
        self.results = []
        self.downloaded_count = 0
        self.index_file = 'index.txt'
        self.csv_file = 'results.csv'
        self.pages_dir = 'downloaded_pages'

        if not os.path.exists(self.pages_dir):
            os.makedirs(self.pages_dir)

        self.load_existing_data()

        self.morph = pymorphy3.MorphAnalyzer()
        self.stopwords = self._load_russian_stopwords()
        self.tokens_dir = 'tokens_output'

        if not os.path.exists(self.tokens_dir):
            os.makedirs(self.tokens_dir)

        self.inverted_index_json = 'inverted_index.json'

        self.tfidf_dir = 'tfidf_output'

        if not os.path.exists(self.tfidf_dir):
            os.makedirs(self.tfidf_dir)

        signal.signal(signal.SIGINT, self.signal_handler)

    def load_existing_data(self):
        if os.path.exists(self.index_file):
            with open(self.index_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            file_num, url = line.split(' ', 1)
                            self.visited_urls.add(url)
                            self.results.append({
                                'file_number': int(file_num),
                                'url': url,
                                'filename': f'page_{int(file_num)}.html',
                                'parent': None  # Родительская страница неизвестна для ранее скачанных
                            })
                            self.downloaded_count = max(self.downloaded_count, int(file_num))
                        except ValueError:
                            continue
            print(f"Загружено {len(self.visited_urls)} уже скачанных страниц")

    def signal_handler(self, sig, frame):
        print("\n\nПрерывание программы...")
        self.save_all_data()
        print("Данные сохранены!")
        sys.exit(0)

    def is_valid_url(self, url):
        parsed = urlparse(url)

        # Проверка на изображения и медиафайлы
        if any(ext in url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp',
                                              '.mp4', '.mp3', '.pdf', '.zip', '.rar',
                                              '.css', '.js', '.ico', '.svg', '.woff', '.ttf']):
            return False

        # Проверка на внутренние ссылки того же сайта (наверное можно убрать, но я не рискнул)
        if parsed.netloc and parsed.netloc != self.base_domain:
            return False

        # Проверка на якоря и параметры сессии
        if '#' in url or 'sessionid' in url.lower() or 'sid=' in url.lower():
            return False

        return True

    def get_page_content(self, url):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()

            content_type = response.headers.get('Content-Type', '').lower()
            if 'text/html' not in content_type and 'text/plain' not in content_type:
                print(f"  Пропущено: не текстовый контент ({content_type})")
                return None

            return response.text
        except requests.RequestException as e:
            print(f"  Ошибка при загрузке: {e}")
            return None

    def extract_links(self, html, base_url):
        soup = BeautifulSoup(html, 'html.parser')
        links = set()

        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(base_url, href)

            if self.is_valid_url(full_url) and full_url not in self.visited_urls:
                links.add(full_url)

        return list(links)

    def crawl(self):
        """
        Основной метод обхода сайта.
        Он использует алгоритм BFS (поиск в ширину) с ограничением глубины.
        """
        remaining = self.min_pages - len(self.visited_urls)
        if remaining <= 0:
            print(f"Требуемое количество страниц ({self.min_pages}) уже скачано!")
            self.save_all_data()
            return

        print(f"Начинаем обход сайта: {self.start_url}")
        print(f"Глубина обхода: {self.max_depth} уровней")
        print(f"Нужно скачать ещё: {remaining} страниц\n")

        # BFS обход с ограничением глубины
        queue = [(self.start_url, 0, None)]  # (url, depth, parent_url)

        while queue and self.downloaded_count < self.min_pages:
            url, depth, parent_url = queue.pop(0)

            if url in self.visited_urls:
                continue

            if depth > self.max_depth:
                continue

            print(f"Страница [{self.downloaded_count + 1}/{self.min_pages}] Глубина {depth}: {url}")

            html = self.get_page_content(url)
            if not html:
                continue

            self.downloaded_count += 1
            filename = f'page_{self.downloaded_count}.html'
            self.save_page(html, filename)

            self.results.append({
                'file_number': self.downloaded_count,
                'url': url,
                'filename': filename,
                'parent': parent_url if parent_url else 'None'
            })

            self.visited_urls.add(url)

            # Извлекаем новые ссылки (если не достигнута максимальная глубина)
            if depth < self.max_depth:
                new_links = self.extract_links(html, url)
                for link in new_links:
                    if link not in self.visited_urls:
                        queue.append((link, depth + 1, url))  # Передаём текущий URL как родительский

            # Сохраняем промежуточные результаты каждые 10 страниц
            if self.downloaded_count % 10 == 0:
                self.save_all_data()
                print(f"Сохранено {self.downloaded_count} страниц\n")

        self.save_all_data()

        if self.downloaded_count >= self.min_pages:
            print(f"\nЦель достигнута: скачано {self.downloaded_count} страниц!")
        else:
            print(f"\nОбход завершён: скачано {self.downloaded_count} страниц (меньше цели, закончились ссылки)")

    def save_page(self, content, filename):
        filepath = os.path.join(self.pages_dir, filename)

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
        except Exception as e:
            print(f"  Ошибка при сохранении {filename}: {e}")

    def save_index_txt(self):
        with open(self.index_file, 'w', encoding='utf-8') as f:
            for result in sorted(self.results, key=lambda x: x['file_number']):
                f.write(f"{result['file_number']} {result['url']}\n")
        print(f"Файл {self.index_file} обновлён")

    def save_csv(self):
        with open(self.csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['file_number', 'url', 'filename', 'parent']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in sorted(self.results, key=lambda x: x['file_number']):
                writer.writerow(row)
        print(f"Файл {self.csv_file} обновлён")

    def save_all_data(self):
        self.save_index_txt()
        self.save_csv()


    ###Отсюда начинается Задание 2:

    def _load_russian_stopwords(self):
        """Базовый набор стоп-слов русского языка (предлоги, союзы, частицы, местоимения)"""
        return {
            'в', 'на', 'за', 'под', 'над', 'при', 'по', 'о', 'об', 'от', 'до', 'из', 'к', 'с', 'у', 'для',
            'через', 'после', 'перед', 'между', 'сквозь', 'около', 'вокруг', 'без', 'кроме', 'через',
            'и', 'или', 'но', 'а', 'да', 'если', 'что', 'чтобы', 'как', 'когда', 'где', 'куда', 'откуда',
            'потому', 'поэтому', 'зато', 'либо', 'нежели', 'будто', 'словно', 'ибо', 'дабы',
            'не', 'ни', 'ли', 'же', 'бы', 'вот', 'ведь', 'пусть', 'давай', 'ка', 'то', 'таки',
            'я', 'ты', 'он', 'она', 'оно', 'мы', 'вы', 'они', 'себя', 'мой', 'твой', 'свой', 'наш', 'ваш',
            'этот', 'тот', 'какой', 'кто', 'что', 'весь', 'вся', 'всё', 'все', 'сам', 'самый',
            'быть', 'был', 'была', 'было', 'были', 'есть', 'будет', 'будут', 'это', 'всего', 'весьма',
            'один', 'два', 'три', 'четыре', 'пять', 'шесть', 'семь', 'восемь', 'девять', 'десять',
            'первый', 'второй', 'третий', 'четвертый', 'пятый'
        }

    def _is_valid_token(self, token):
        token = token.strip(string.punctuation + '«»""\'\'–—…').lower()

        if not token or len(token) < 2 or len(token) > 50:
            return False

        if token in self.stopwords:
            return False

        if token.isdigit():
            return False

        if re.search(r'[a-zA-Zа-яА-Я]', token) and re.search(r'\d', token):
            return False

        if re.match(r'^[<>/{}[\]\\|@#$%^&*=~`]+$', token):
            return False

        if re.match(r'^([a-zA-Zа-яА-Я])\1{2,}$', token):
            return False

        if not re.match(r'^[a-zA-Zа-яА-ЯёЁ]+$', token):
            return False

        return True

    def _extract_text_from_html(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')

        for tag in soup(['script', 'style', 'meta', 'noscript', 'header', 'footer', 'nav']):
            tag.decompose()

        text = soup.get_text(separator=' ', strip=True)
        text = re.sub(r'\s+', ' ', text)
        return text

    def _tokenize_text(self, text):
        tokens = re.findall(r'[a-zA-Zа-яА-ЯёЁ]+', text)
        return tokens

    def _lemmatize_token(self, token):
        parse = self.morph.parse(token)[0]
        return parse.normal_form

    def process_downloaded_pages(self):
        print(f"\nНачинаю обработку {len(self.results)} скачанных страниц...")

        pages_dir = os.path.join(os.getcwd(), self.pages_dir)
        processed_count = 0

        for result in self.results:
            filepath = os.path.join(pages_dir, result['filename'])
            file_number = result['file_number']

            if not os.path.exists(filepath):
                print(f"Файл не найден: {filepath}")
                continue

            page_output_dir = os.path.join(self.tokens_dir, f"page_{file_number:03d}")
            os.makedirs(page_output_dir, exist_ok=True)

            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    html_content = f.read()

                text = self._extract_text_from_html(html_content)
                raw_tokens = self._tokenize_text(text)
                all_tokens = set()
                lemma_to_tokens = defaultdict(set)

                for token in raw_tokens:
                    if self._is_valid_token(token):
                        all_tokens.add(token)
                        lemma = self._lemmatize_token(token)
                        lemma_to_tokens[lemma].add(token)

                self._save_tokens_per_page(all_tokens, page_output_dir, file_number)
                self._save_lemmas_per_page(lemma_to_tokens, page_output_dir, file_number)

                processed_count += 1
                if processed_count % 10 == 0:
                    print(f"Обработано страниц: {processed_count}/{len(self.results)}")

            except Exception as e:
                print(f"Ошибка при обработке {result['filename']}: {e}")
                continue

        print(f"\nОбработано {processed_count} страниц")
        print(f"Результаты сохранены в папке: {self.tokens_dir}/")
        return processed_count

    def _save_tokens_per_page(self, tokens_set, output_dir, file_number):
        filepath = os.path.join(output_dir, 'tokens.txt')

        with open(filepath, 'w', encoding='utf-8') as f:
            for token in sorted(tokens_set):
                f.write(f"{token}\n")

        print(f"  ✓ Page {file_number}: {len(tokens_set)} токенов → {filepath}")

    def _save_lemmas_per_page(self, lemma_dict, output_dir, file_number):
        filepath = os.path.join(output_dir, 'lemmas.txt')

        with open(filepath, 'w', encoding='utf-8') as f:
            for lemma in sorted(lemma_dict.keys()):
                tokens = sorted(lemma_dict[lemma])
                line = f"{lemma} {' '.join(tokens)}\n"
                f.write(line)

        print(f"Page {file_number}: {len(lemma_dict)} лемм → {filepath}")

    def build_inverted_index(self, use_lemmas=True):

        inverted_index = defaultdict(set)

        for result in self.results:
            file_number = result['file_number']

            if use_lemmas:
                terms_file = os.path.join(
                    self.tokens_dir,
                    f"page_{file_number:03d}",
                    'lemmas.txt'
                )
            else:
                terms_file = os.path.join(
                    self.tokens_dir,
                    f"page_{file_number:03d}",
                    'tokens.txt'
                )

            if not os.path.exists(terms_file):
                print(f"Файл не найден: {terms_file}")
                continue

            try:
                with open(terms_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        if use_lemmas:
                            term = line.split()[0]
                        else:
                            term = line

                        inverted_index[term].add(file_number)

            except Exception as e:
                print(f"Ошибка при чтении {terms_file}: {e}")
                continue

        inverted_index = {
            term: sorted(list(doc_ids))
            for term, doc_ids in inverted_index.items()
        }

        self._save_inverted_index(inverted_index)

        print(f"\nИнвертированный индекс построен!")
        print(f"   Уникальных терминов: {len(inverted_index)}")
        print(f"   Документы в индексе: {len(self.results)}")

        return inverted_index

    def _save_inverted_index(self, inverted_index):
        with open(self.inverted_index_json, 'w', encoding='utf-8') as f:
            json.dump(inverted_index, f, ensure_ascii=False, indent=2)

        print(f"Сохранено в {self.inverted_index_json}")

    def search_by_term(self, term, use_lemmas=True):
        if not os.path.exists(self.inverted_index_json):
            print("Инвертированный индекс не найден!")
            return []

        with open(self.inverted_index_json, 'r', encoding='utf-8') as f:
            inverted_index = json.load(f)

        term = term.lower()

        if use_lemmas:
            term = self._lemmatize_token(term)

        if term in inverted_index:
            doc_ids = inverted_index[term]
            print(f"\nТермин '{term}' найден в документах: {doc_ids}")

            for doc_id in doc_ids:
                for result in self.results:
                    if result['file_number'] == doc_id:
                        print(f"{doc_id}: {result['url']}")
                        break

            return doc_ids
        else:
            print(f"\nТермин '{term}' не найден ни в одном документе")
            return []

    def search_by_multiple_terms(self, terms, operator='AND', exclude_terms=None):
        if not os.path.exists(self.inverted_index_json):
            print("Инвертированный индекс не найден!")
            return []

        with open(self.inverted_index_json, 'r', encoding='utf-8') as f:
            inverted_index = json.load(f)

        normalized_terms = []
        for term in terms:
            term = term.lower().strip()
            if term:
                term = self._lemmatize_token(term)
                normalized_terms.append(term)

        normalized_exclude = []
        if exclude_terms:
            for term in exclude_terms:
                term = term.lower().strip()
                if term:
                    term = self._lemmatize_token(term)
                    normalized_exclude.append(term)

        doc_sets = []
        for term in normalized_terms:
            if term in inverted_index:
                doc_sets.append(set(inverted_index[term]))
            else:
                doc_sets.append(set())

        if not doc_sets:
            result = set()
        elif operator == 'AND':
            result = set.intersection(*doc_sets)
            print(f"\nПоиск AND: найдено {len(result)} документов")
        elif operator == 'OR':
            result = set.union(*doc_sets)
            print(f"\nПоиск OR: найдено {len(result)} документов")
        else:
            print("Неверный оператор! Используйте 'AND' или 'OR'")
            return []

        if normalized_exclude:
            exclude_sets = []
            for term in normalized_exclude:
                if term in inverted_index:
                    exclude_sets.append(set(inverted_index[term]))
            if exclude_sets:
                exclude_docs = set.union(*exclude_sets)
                result = result - exclude_docs
                print(f"Исключено документов: {len(exclude_docs)}")
                print(f"Итоговый результат: {len(result)} документов")

        if result:
            print("\nРезультаты:")
            for doc_id in sorted(result):
                for result_item in self.results:
                    if result_item['file_number'] == doc_id:
                        print(f"   {doc_id}: {result_item['url']}")
                        break
        else:
            print("\nНичего не найдено")

        return sorted(list(result))

    def _get_term_docs(self, term, inverted_index):
        term = term.lower().strip()
        if not term:
            return set()
        term = self._lemmatize_token(term)
        if term in inverted_index:
            return set(inverted_index[term])
        return set()

    def _tokenize_query(self, query_string):
        pattern = r'\(|\)|AND|OR|NOT|"[^"]+"|\w+'
        tokens = re.findall(pattern, query_string, re.IGNORECASE)
        return [token.strip() for token in tokens if token.strip() and token not in ['', ' ', '\t']]

    def _parse_query_expression(self, tokens, inverted_index, depth=0):
        if not tokens:
            return set()

        indent = "  " * depth
        result_sets = []
        exclude_sets = []
        current_operator = 'OR'
        i = 0

        while i < len(tokens):
            token = tokens[i]

            if token == ')':
                i += 1
                continue

            if token == '(':
                depth_check = 1
                j = i + 1
                while j < len(tokens) and depth_check > 0:
                    if tokens[j] == '(':
                        depth_check += 1
                    elif tokens[j] == ')':
                        depth_check -= 1
                    j += 1

                sub_expression = tokens[i + 1:j - 1]
                sub_result = self._parse_query_expression(sub_expression, inverted_index, depth + 1)

                print(f"{indent}Группа найдена: {len(sub_result)} документов\n")

                if current_operator == 'OR':
                    result_sets.append(sub_result)
                elif current_operator == 'AND':
                    if result_sets:
                        prev_count = len(result_sets[-1])
                        result_sets[-1] = result_sets[-1].intersection(sub_result)
                        print(f"{indent}AND операция: {prev_count} -> {len(result_sets[-1])} документов")
                    else:
                        result_sets.append(sub_result)
                i = j
                continue

            if token.upper() == 'OR':
                current_operator = 'OR'
                print(f"{indent}Оператор: OR")
                i += 1
                continue

            if token.upper() == 'AND':
                current_operator = 'AND'
                print(f"{indent}Оператор: AND")
                i += 1
                continue

            if token.upper() == 'NOT':
                # Всё, что после NOT - это исключения
                current_operator = 'EXCLUDE'
                print(f"{indent}Оператор: NOT (режим исключения)")
                i += 1
                continue

            if token.startswith('-'):
                exclude_term = token[1:]
                if exclude_term == '(':
                    depth_check = 1
                    j = i + 1
                    while j < len(tokens) and depth_check > 0:
                        if tokens[j] == '(':
                            depth_check += 1
                        elif tokens[j] == ')':
                            depth_check -= 1
                        j += 1
                    sub_expression = tokens[i + 2:j - 1]
                    sub_result = self._parse_query_expression(sub_expression, inverted_index, depth + 1)
                    exclude_sets.append(sub_result)
                    print(f"{indent}Исключена группа (-): {len(sub_result)} документов")
                    i = j
                    continue
                else:
                    exclude_docs = self._get_term_docs(exclude_term, inverted_index)
                    exclude_sets.append(exclude_docs)
                    print(f"{indent}NOT операция (-): {len(exclude_docs)} документов")
                    i += 1
                    continue

            term_docs = self._get_term_docs(token, inverted_index)
            print(f"{indent}Термин '{token}': {len(term_docs)} документов")

            if current_operator == 'EXCLUDE':
                exclude_sets.append(term_docs)
            elif current_operator == 'OR':
                result_sets.append(term_docs)
            elif current_operator == 'AND':
                if result_sets:
                    prev_count = len(result_sets[-1])
                    result_sets[-1] = result_sets[-1].intersection(term_docs)
                    print(f"{indent}AND операция: {prev_count} -> {len(result_sets[-1])} документов")
                else:
                    result_sets.append(term_docs)
            i += 1

        if not result_sets:
            final_result = set()
        else:
            final_result = result_sets[0]
            for idx, result_set in enumerate(result_sets[1:], 1):
                prev_count = len(final_result)
                final_result = final_result.union(result_set)
                print(f"{indent}OR операция ({idx}): {prev_count} -> {len(final_result)} документов")

        if exclude_sets:
            exclude_docs = set()
            for exc_set in exclude_sets:
                exclude_docs = exclude_docs.union(exc_set)
            prev_count = len(final_result)
            final_result = final_result - exclude_docs
            print(
                f"{indent}NOT операция: {prev_count} -> {len(final_result)} документов (исключено {len(exclude_docs)})")

        print(f"{indent}Итог выражения: {len(final_result)} документов")
        return final_result

    def _print_search_results(self, doc_ids):
        if not doc_ids:
            print("\nНичего не найдено")
            return

        print(f"\nНайдено {len(doc_ids)} документов:")
        for doc_id in sorted(doc_ids):
            for result_item in self.results:
                if result_item['file_number'] == doc_id:
                    print(f"   {doc_id}: {result_item['url']}")
                    break

    def get_term_statistics(self, top_n=20):
        if not os.path.exists(self.inverted_index_json):
            print("Инвертированный индекс не найден!")
            return {}

        with open(self.inverted_index_json, 'r', encoding='utf-8') as f:
            inverted_index = json.load(f)
        term_freq = {
            term: len(doc_ids)
            for term, doc_ids in inverted_index.items()
        }
        sorted_terms = sorted(term_freq.items(), key=lambda x: x[1], reverse=True)
        print(f"\nПервые {top_n} терминов по частотности в документах:")
        for i, (term, freq) in enumerate(sorted_terms[:top_n], 1):
            print(f"{i:3}. {term:30} → {freq} док.")

        return {
            'total_terms': len(term_freq),
            'top_terms': sorted_terms[:top_n],
            'avg_docs_per_term': sum(term_freq.values()) / len(term_freq) if term_freq else 0
        }

    def search_query(self, query_string):
        if not query_string or not query_string.strip():
            print("Пустой поисковый запрос!")
            return []

        if not os.path.exists(self.inverted_index_json):
            print("Инвертированный индекс не найден!")
            return []

        print(f"\nПоисковый запрос: \"{query_string}\"")
        tokens = self._tokenize_query(query_string)

        if not tokens:
            return []

        with open(self.inverted_index_json, 'r', encoding='utf-8') as f:
            inverted_index = json.load(f)
        result_docs = self._parse_query_expression(tokens, inverted_index, depth=0)
        self._print_search_results(result_docs)

        return sorted(list(result_docs))

    #Задание 4
    def _calculate_tf(self, tokens):
        term_counts = Counter(tokens)
        total_terms = len(tokens)

        if total_terms == 0:
            return {}

        tf = {}
        for term, count in term_counts.items():
            tf[term] = count / total_terms

        return tf

    def _calculate_idf(self, term, total_docs, docs_with_term):
        if docs_with_term == 0:
            return 0.0

        idf = math.log(total_docs / docs_with_term)
        return idf

    def _calculate_tfidf(self, tf, idf):
        return tf * idf

    def _build_corpus_stats(self):
        term_doc_count = Counter()
        lemma_doc_count = Counter()

        for result in self.results:
            file_number = result['file_number']
            tokens_file = os.path.join(
                self.tokens_dir,
                f"page_{file_number:03d}",
                'tokens.txt'
            )
            lemmas_file = os.path.join(
                self.tokens_dir,
                f"page_{file_number:03d}",
                'lemmas.txt'
            )

            doc_terms = set()
            doc_lemmas = set()

            if os.path.exists(tokens_file):
                with open(tokens_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        term = line.strip()
                        if term:
                            doc_terms.add(term)

            if os.path.exists(lemmas_file):
                with open(lemmas_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        parts = line.strip().split()
                        if parts:
                            lemma = parts[0]
                            doc_lemmas.add(lemma)

            for term in doc_terms:
                term_doc_count[term] += 1

            for lemma in doc_lemmas:
                lemma_doc_count[lemma] += 1

        return term_doc_count, lemma_doc_count

    def _extract_raw_tokens_from_html(self, html_content):
        text = self._extract_text_from_html(html_content)
        tokens = self._tokenize_text(text)
        filtered_tokens = []
        for token in tokens:
            if self._is_valid_token(token):
                filtered_tokens.append(token)
        return filtered_tokens

    def _extract_raw_lemmas_from_html(self, html_content):
        tokens = self._extract_raw_tokens_from_html(html_content)
        lemmas = []
        for token in tokens:
            lemma = self._lemmatize_token(token)
            lemmas.append(lemma)
        return lemmas

    def process_tfidf(self):
        print("\nРасчет TF-IDF для всех документов...")

        total_docs = len(self.results)

        if total_docs == 0:
            print("Нет документов для обработки!")
            return

        term_doc_count, lemma_doc_count = self._build_corpus_stats()

        print(f"Всего документов: {total_docs}")
        print(f"Уникальных терминов в корпусе: {len(term_doc_count)}")
        print(f"Уникальных лемм в корпусе: {len(lemma_doc_count)}")

        pages_dir = os.path.join(os.getcwd(), self.pages_dir)
        processed_count = 0

        for result in self.results:
            file_number = result['file_number']
            filepath = os.path.join(pages_dir, result['filename'])

            if not os.path.exists(filepath):
                print(f"Файл не найден: {filepath}")
                continue

            page_output_dir = os.path.join(self.tfidf_dir, f"page_{file_number:03d}")
            os.makedirs(page_output_dir, exist_ok=True)

            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    html_content = f.read()

                raw_tokens = self._extract_raw_tokens_from_html(html_content)
                raw_lemmas = self._extract_raw_lemmas_from_html(html_content)

                tf_tokens = self._calculate_tf(raw_tokens)
                tf_lemmas = self._calculate_tf(raw_lemmas)

                tfidf_tokens = {}
                for term, tf in tf_tokens.items():
                    docs_with_term = term_doc_count.get(term, 0)
                    idf = self._calculate_idf(term, total_docs, docs_with_term)
                    tfidf = self._calculate_tfidf(tf, idf)
                    tfidf_tokens[term] = (idf, tfidf)

                tfidf_lemmas = {}
                for lemma, tf in tf_lemmas.items():
                    docs_with_lemma = lemma_doc_count.get(lemma, 0)
                    idf = self._calculate_idf(lemma, total_docs, docs_with_lemma)
                    tfidf = self._calculate_tfidf(tf, idf)
                    tfidf_lemmas[lemma] = (idf, tfidf)

                self._save_tfidf_tokens(tfidf_tokens, page_output_dir, file_number)
                self._save_tfidf_lemmas(tfidf_lemmas, page_output_dir, file_number)

                processed_count += 1
                if processed_count % 10 == 0:
                    print(f"Обработано страниц: {processed_count}/{len(self.results)}")

            except Exception as e:
                print(f"Ошибка при обработке {result['filename']}: {e}")
                continue

        print(f"\nГотово! Обработано {processed_count} страниц")
        print(f"Результаты сохранены в папке: {self.tfidf_dir}/")
        return processed_count

    def _save_tfidf_tokens(self, tfidf_data, output_dir, file_number):
        filepath = os.path.join(output_dir, 'tfidf_tokens.txt')

        with open(filepath, 'w', encoding='utf-8') as f:
            for term in sorted(tfidf_data.keys()):
                idf, tfidf = tfidf_data[term]
                f.write(f"{term} {idf:.6f} {tfidf:.6f}\n")

        print(f"  Page {file_number}: {len(tfidf_data)} терминов -> {filepath}")

    def _save_tfidf_lemmas(self, tfidf_data, output_dir, file_number):
        filepath = os.path.join(output_dir, 'tfidf_lemmas.txt')

        with open(filepath, 'w', encoding='utf-8') as f:
            for lemma in sorted(tfidf_data.keys()):
                idf, tfidf = tfidf_data[lemma]
                f.write(f"{lemma} {idf:.6f} {tfidf:.6f}\n")

        print(f"  Page {file_number}: {len(tfidf_data)} лемм -> {filepath}")

    def get_tfidf_statistics(self):
        print("\nСтатистика TF-IDF по корпусу:")
        print("=" * 50)

        all_tfidf_tokens = []
        all_tfidf_lemmas = []

        for result in self.results:
            file_number = result['file_number']
            tokens_file = os.path.join(
                self.tfidf_dir,
                f"page_{file_number:03d}",
                'tfidf_tokens.txt'
            )
            lemmas_file = os.path.join(
                self.tfidf_dir,
                f"page_{file_number:03d}",
                'tfidf_lemmas.txt'
            )

            if os.path.exists(tokens_file):
                with open(tokens_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        parts = line.strip().split()
                        if len(parts) >= 3:
                            all_tfidf_tokens.append(float(parts[2]))

            if os.path.exists(lemmas_file):
                with open(lemmas_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        parts = line.strip().split()
                        if len(parts) >= 3:
                            all_tfidf_lemmas.append(float(parts[2]))

        if all_tfidf_tokens:
            avg_tfidf_tokens = sum(all_tfidf_tokens) / len(all_tfidf_tokens)
            max_tfidf_tokens = max(all_tfidf_tokens)
            min_tfidf_tokens = min(all_tfidf_tokens)
            print(f"Термины:")
            print(f"  Средний TF-IDF: {avg_tfidf_tokens:.6f}")
            print(f"  Максимальный TF-IDF: {max_tfidf_tokens:.6f}")
            print(f"  Минимальный TF-IDF: {min_tfidf_tokens:.6f}")
            print(f"  Всего значений: {len(all_tfidf_tokens)}")

        if all_tfidf_lemmas:
            avg_tfidf_lemmas = sum(all_tfidf_lemmas) / len(all_tfidf_lemmas)
            max_tfidf_lemmas = max(all_tfidf_lemmas)
            min_tfidf_lemmas = min(all_tfidf_lemmas)
            print(f"\nЛеммы:")
            print(f"  Средний TF-IDF: {avg_tfidf_lemmas:.6f}")
            print(f"  Максимальный TF-IDF: {max_tfidf_lemmas:.6f}")
            print(f"  Минимальный TF-IDF: {min_tfidf_lemmas:.6f}")
            print(f"  Всего значений: {len(all_tfidf_lemmas)}")

        return {
            'tokens': {
                'avg': avg_tfidf_tokens if all_tfidf_tokens else 0,
                'max': max_tfidf_tokens if all_tfidf_tokens else 0,
                'min': min_tfidf_tokens if all_tfidf_tokens else 0,
                'count': len(all_tfidf_tokens)
            },
            'lemmas': {
                'avg': avg_tfidf_lemmas if all_tfidf_lemmas else 0,
                'max': max_tfidf_lemmas if all_tfidf_lemmas else 0,
                'min': min_tfidf_lemmas if all_tfidf_lemmas else 0,
                'count': len(all_tfidf_lemmas)
            }
        }

if __name__ == "__main__":
    START_URL = "https://habr.com/ru/news/1000374/"
    MIN_PAGES = 150
    MAX_DEPTH = 3

    #print(f"Начальный URL: {START_URL}")
    #print(f"Цель: {MIN_PAGES} страниц")
    #print(f"Максимальная глубина: {MAX_DEPTH} уровней")

    spider = WebSpider(START_URL, min_pages=MIN_PAGES, max_depth=MAX_DEPTH)
    #spider.crawl()

    #spider.process_downloaded_pages()

    #spider.build_inverted_index()
    # spider.search_query("(SSO AND python) OR ИИ NOT (git OR кабан)")
    # spider.search_query("ИТ OR ИП")
    #
    # spider.search_query("git -python")

    spider.process_tfidf()
    spider.get_tfidf_statistics()

    print("Всё!")