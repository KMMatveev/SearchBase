from flask import Flask, render_template, request, jsonify
import os
import sys
import json
import math

app = Flask(__name__)
app.config['SECRET_KEY'] = 'search-system-key'

spider_instance = None


def init_spider(spider):
    global spider_instance
    spider_instance = spider


@app.route('/')
def index():
    return render_template('search.html', results=None, query='')


@app.route('/search')
def search():
    query = request.args.get('q', '')
    page = request.args.get('page', 1, type=int)

    if not query:
        return render_template('search.html', results=None, query='')

    if spider_instance is None:
        return render_template('search.html',
                               results={'error': 'Поисковая система не инициализирована'},
                               query=query)

    search_results = spider_instance.vector_search(query, page=page, per_page=10)

    return render_template('search.html', results=search_results, query=query)


@app.route('/api/search')
def api_search():
    query = request.args.get('q', '')
    page = request.args.get('page', 1, type=int)

    if spider_instance is None:
        return jsonify({'error': 'Поисковая система не инициализирована'})

    search_results = spider_instance.vector_search(query, page=page, per_page=10)

    return jsonify(search_results)


def run_server(spider, host='0.0.0.0', port=5000, debug=False):
    global spider_instance
    init_spider(spider)

    print("\n" + "=" * 50)
    print("Поисковая система")
    print("=" * 50)

    if not os.path.exists('inverted_index.json'):
        print("Ошибка: Инвертированный индекс не найден!")
        print("Сначала выполните сбор и обработку данных")
        print("Раскомментируйте в main.py:")
        print("  spider.crawl()")
        print("  spider.process_downloaded_pages()")
        print("  spider.build_inverted_index()")
        print("  spider.process_tfidf()")
        sys.exit(1)

    if not os.path.exists('vector_index.json'):
        print("Построение векторного индекса...")
        vector_index = spider._build_vector_index()
        with open(spider.vector_index_file, 'w', encoding='utf-8') as f:
            json.dump(vector_index, f, ensure_ascii=False, indent=2)
        print("Векторный индекс построен!")

    print(f"\nЗапуск веб-сервера...")
    print(f"URL: http://localhost:{port}")
    print(f"Нажмите Ctrl+C для остановки\n")

    app.run(host=host, port=port, debug=debug)