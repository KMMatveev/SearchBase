import requests
from bs4 import BeautifulSoup
import csv
import os
import signal
import sys
from urllib.parse import urljoin, urlparse


class WebSpider:
    """
    Класс веб-паука для обхода сайта и скачивания страниц.
    Лейсан, этот класс содержит всю логику работы паука.
    """

    def __init__(self, start_url, min_pages=100, max_depth=3):
        """
        Лейсан, инициализация паука.
        Здесь мы задаём начальные параметры и создаём необходимые структуры данных.
        """
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

        # Создаём директорию для страниц
        if not os.path.exists(self.pages_dir):
            os.makedirs(self.pages_dir)

        # Загружаем уже скачанные страницы
        self.load_existing_data()

        # Настройка обработчика прерываний
        signal.signal(signal.SIGINT, self.signal_handler)

    def load_existing_data(self):
        """
        Лейсан, этот метод загружает информацию о ранее скачанных страницах из index.txt.
        Это позволяет продолжить работу с того места, где мы остановились.
        """
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
        """
        Лейсан, этот метод срабатывает при нажатии Ctrl+C.
        Он сохраняет все данные перед завершением программы.
        """
        print("\n\nПрерывание программы...")
        self.save_all_data()
        print("Данные сохранены!")
        sys.exit(0)

    def is_valid_url(self, url):
        """
        Лейсан, этот метод проверяет, является ли ссылка подходящей для скачивания.
        Мы исключаем изображения, медиафайлы и внешние ссылки.
        """
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
        """
        Лейсан, этот метод скачивает содержимое страницы по указанному URL.
        Он использует requests с заголовком User-Agent для имитации браузера.
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()

            # Проверка на текстовый контент
            content_type = response.headers.get('Content-Type', '').lower()
            if 'text/html' not in content_type and 'text/plain' not in content_type:
                print(f"  Пропущено: не текстовый контент ({content_type})")
                return None

            return response.text
        except requests.RequestException as e:
            print(f"  Ошибка при загрузке: {e}")
            return None

    def extract_links(self, html, base_url):
        """
        Лейсан, этот метод извлекает все ссылки со страницы.
        Он парсит HTML с помощью BeautifulSoup и находит все теги <a>.
        """
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
        Лейсан, это основной метод обхода сайта.
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

            # Сохраняем полную HTML страницу
            self.downloaded_count += 1
            filename = f'page_{self.downloaded_count}.html'
            self.save_page(html, filename)

            # Сохраняем информацию о странице, включая родительскую
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

        # Финальное сохранение
        self.save_all_data()

        if self.downloaded_count >= self.min_pages:
            print(f"\nЦель достигнута: скачано {self.downloaded_count} страниц!")
        else:
            print(f"\nОбход завершён: скачано {self.downloaded_count} страниц (меньше цели, закончились ссылки)")

    def save_page(self, content, filename):
        """
        Лейсан, этот метод сохраняет содержимое страницы в HTML файл.
        Файлы сохраняются в папку downloaded_pages.
        """
        filepath = os.path.join(self.pages_dir, filename)

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
        except Exception as e:
            print(f"  Ошибка при сохранении {filename}: {e}")

    def save_index_txt(self):
        """
        Лейсан, этот метод создаёт файл index.txt.
        В нём хранится номер файла и соответствующая ссылка.
        """
        with open(self.index_file, 'w', encoding='utf-8') as f:
            for result in sorted(self.results, key=lambda x: x['file_number']):
                f.write(f"{result['file_number']} {result['url']}\n")
        print(f"Файл {self.index_file} обновлён")

    def save_csv(self):
        """
        Лейсан, этот метод создаёт файл results.csv.
        В нём хранится таблица с номером файла, ссылкой, именем файла и родительской страницей.
        """
        with open(self.csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['file_number', 'url', 'filename', 'parent']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in sorted(self.results, key=lambda x: x['file_number']):
                writer.writerow(row)
        print(f"Файл {self.csv_file} обновлён")

    def save_all_data(self):
        """
        Лейсан, этот метод сохраняет все данные одновременно.
        Он вызывает методы сохранения для index.txt и results.csv.
        """
        self.save_index_txt()
        self.save_csv()


# Пример использования
if __name__ == "__main__":
    START_URL = "https://habr.com/ru/news/1000374/"
    MIN_PAGES = 150
    MAX_DEPTH = 3

    print(f"Начальный URL: {START_URL}")
    print(f"Цель: {MIN_PAGES} страниц")
    print(f"Максимальная глубина: {MAX_DEPTH} уровней")

    spider = WebSpider(START_URL, min_pages=MIN_PAGES, max_depth=MAX_DEPTH)
    spider.crawl()

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