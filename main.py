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
        self.tokens_file = 'tokens.txt'
        self.lemmas_file = 'lemmas.txt'

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

        # Удаляем неинформативные теги
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

        all_tokens = set()
        lemma_to_tokens = defaultdict(set)

        pages_dir = os.path.join(os.getcwd(), self.pages_dir)

        for result in self.results:
            filepath = os.path.join(pages_dir, result['filename'])

            if not os.path.exists(filepath):
                print(f"Файл не найден: {filepath}")
                continue

            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    html_content = f.read()

                text = self._extract_text_from_html(html_content)
                raw_tokens = self._tokenize_text(text)
                for token in raw_tokens:
                    if self._is_valid_token(token):
                        all_tokens.add(token)
                        lemma = self._lemmatize_token(token)
                        lemma_to_tokens[lemma].add(token)

            except Exception as e:
                print(f"Ошибка при обработке {result['filename']}: {e}")
                continue

        self._save_tokens(all_tokens)
        self._save_lemmas(lemma_to_tokens)

        print(f"Готово! Обработано токенов: {len(all_tokens)}, уникальных лемм: {len(lemma_to_tokens)}")
        return all_tokens, lemma_to_tokens

    def _save_tokens(self, tokens_set):
        with open(self.tokens_file, 'w', encoding='utf-8') as f:
            for token in sorted(tokens_set):
                f.write(f"{token}\n")
        print(f"Сохранено {len(tokens_set)} токенов в {self.tokens_file}")

    def _save_lemmas(self, lemma_dict):
        with open(self.lemmas_file, 'w', encoding='utf-8') as f:
            # Сортируем по леммам
            for lemma in sorted(lemma_dict.keys()):
                tokens = sorted(lemma_dict[lemma])  # Сортируем токены
                line = f"{lemma} {' '.join(tokens)}\n"
                f.write(line)
        print(f"Сохранено {len(lemma_dict)} лемм в {self.lemmas_file}")

if __name__ == "__main__":
    START_URL = "https://habr.com/ru/news/1000374/"
    MIN_PAGES = 150
    MAX_DEPTH = 3

    print(f"Начальный URL: {START_URL}")
    print(f"Цель: {MIN_PAGES} страниц")
    print(f"Максимальная глубина: {MAX_DEPTH} уровней")

    spider = WebSpider(START_URL, min_pages=MIN_PAGES, max_depth=MAX_DEPTH)
    spider.crawl()

    spider.process_downloaded_pages()

    print("Всё!")

"""
Бро, я кратко попытался описать:

1. Init:
   - Создаётся объект класса WebSpider с указанным начальным URL
   - Загружаются данные о ранее скачанных страницах (если есть)
   - Создаётся очередь для обхода сайта

2. Сам обход сайта (BFS(beautiful soup) - поиск в ширину):
   - Берём первую ссылку из очереди
   - Скачиваем страницу по этой ссылке
   - Сохраняем HTML-код в файл
   - Извлекаем все ссылки со страницы
   - Добавляем новые ссылки в очередь с увеличенной глубиной
   - Повторяем до достижения цели (150 страниц) или исчерпания ссылок

3. Очеред обработки:
   Каждый элемент очереди содержит:
   - url: адрес страницы для скачивания
   - depth: глубина обхода (0, 1, 2, 3)
   - parent_url: ссылка на страницу, откуда была извлечена эта ссылка

4. Сохранение:
   - Каждая страница сохраняется как page_N.html в папку downloaded_pages
   - Информация о страницах сохраняется в двух файлах:
     * index.txt - простой список "номер ссылка"
     * results.csv - таблица с колонками: номер, ссылка, имя файла, родительская страница(это я от себя добавил, по идее не нужно)

5. Проверка повторов:
   - Используется множество visited_urls для отслеживания уже посещённых страниц
   - Ссылки проверяются на валидность (не медиафайлы, внутренние ссылки сайта)

6. Ограничения задаются в мейне:
   - Максимум страниц
   - Глубина обхода не более X уровней
"""