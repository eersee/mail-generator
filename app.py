"""
Flask backend для генерации писем из шаблона и CSV
"""
from flask import Flask, request, send_file, jsonify
from flask_cors import CORS
from docxtpl import DocxTemplate
from docx2pdf import convert
import pandas as pd
import os
import re
import io
import zipfile
from werkzeug.utils import secure_filename
import tempfile
import shutil
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Конфигурация
UPLOAD_FOLDER = tempfile.gettempdir()
ALLOWED_EXTENSIONS = {'docx', 'csv'}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def sanitize_filename(filename):
    """Очистка имени файла от опасных символов"""
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def detect_csv_encoding(file_path):
    """Определение кодировки CSV"""
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
    for encoding in encodings:
        try:
            pd.read_csv(file_path, sep=';', encoding=encoding, nrows=1)
            return encoding
        except:
            continue
    return 'utf-8'

def get_csv_columns(file_path):
    """Получение списка столбцов из CSV"""
    encoding = detect_csv_encoding(file_path)
    try:
        df = pd.read_csv(file_path, sep=';', encoding=encoding, nrows=0)
        return list(df.columns)
    except Exception as e:
        return None

@app.route('/api/preview-template', methods=['POST'])
def preview_template():
    """Получить переменные из шаблона DOCX"""
    try:
        if 'template' not in request.files:
            return jsonify({'error': 'Шаблон не загружен'}), 400
        
        template_file = request.files['template']
        if template_file.filename == '':
            return jsonify({'error': 'Файл шаблона пуст'}), 400
        
        if not allowed_file(template_file.filename):
            return jsonify({'error': 'Недопустимый формат шаблона'}), 400
        
        # Сохраняем временный файл
        temp_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(template_file.filename))
        template_file.save(temp_path)
        
        try:
            # Загружаем шаблон и извлекаем переменные
            doc = DocxTemplate(temp_path)
            variables = list(doc.get_undeclared_variables())
            os.remove(temp_path)
            
            return jsonify({
                'success': True,
                'variables': variables
            })
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return jsonify({'error': f'Ошибка при чтении шаблона: {str(e)}'}), 400
    
    except Exception as e:
        return jsonify({'error': f'Неизвестная ошибка: {str(e)}'}), 500

@app.route('/api/preview-csv', methods=['POST'])
def preview_csv():
    """Получить столбцы из CSV"""
    try:
        if 'csv' not in request.files:
            return jsonify({'error': 'CSV не загружен'}), 400
        
        csv_file = request.files['csv']
        if csv_file.filename == '':
            return jsonify({'error': 'Файл CSV пуст'}), 400
        
        if not allowed_file(csv_file.filename):
            return jsonify({'error': 'Недопустимый формат CSV'}), 400
        
        # Сохраняем временный файл
        temp_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(csv_file.filename))
        csv_file.save(temp_path)
        
        try:
            encoding = detect_csv_encoding(temp_path)
            df = pd.read_csv(temp_path, sep=';', encoding=encoding)
            
            columns = list(df.columns)
            preview_data = df.head(3).values.tolist()
            row_count = len(df)
            
            os.remove(temp_path)
            
            return jsonify({
                'success': True,
                'columns': columns,
                'preview': preview_data,
                'rowCount': row_count
            })
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return jsonify({'error': f'Ошибка при чтении CSV: {str(e)}'}), 400
    
    except Exception as e:
        return jsonify({'error': f'Неизвестная ошибка: {str(e)}'}), 500

@app.route('/api/generate', methods=['POST'])
def generate_documents():
    """Генерация документов и создание архива"""
    try:
        # Проверка файлов
        if 'template' not in request.files or 'csv' not in request.files:
            return jsonify({'error': 'Шаблон и CSV обязательны'}), 400
        
        template_file = request.files['template']
        csv_file = request.files['csv']
        mapping = request.form.get('mapping', '{}')
        
        if template_file.filename == '' or csv_file.filename == '':
            return jsonify({'error': 'Файлы не выбраны'}), 400
        
        # Парсим маппинг
        import json
        try:
            field_mapping = json.loads(mapping) if mapping != '{}' else {}
        except:
            field_mapping = {}
        
        # Создаем временную директорию для работы
        work_dir = tempfile.mkdtemp()
        
        try:
            # Сохраняем загруженные файлы
            template_path = os.path.join(work_dir, secure_filename(template_file.filename))
            csv_path = os.path.join(work_dir, secure_filename(csv_file.filename))
            
            template_file.save(template_path)
            csv_file.save(csv_path)
            
            # Загружаем данные из CSV
            encoding = detect_csv_encoding(csv_path)
            data = pd.read_csv(csv_path, sep=';', encoding=encoding)
            
            # Создаем директории для вывода
            output_folder = os.path.join(work_dir, "output_docs")
            pdf_folder = os.path.join(output_folder, "pdf_files")
            os.makedirs(pdf_folder, exist_ok=True)
            
            # Загружаем шаблон
            doc_template = DocxTemplate(template_path)
            
            # Генерируем документы
            success_count = 0
            error_list = []
            
            for idx, row in data.iterrows():
                try:
                    # Подготавливаем контекст
                    context = {}
                    for var_name in doc_template.get_undeclared_variables():
                        # Проверяем маппинг
                        csv_column = field_mapping.get(var_name, var_name)
                        if csv_column in row.index:
                            context[var_name] = str(row[csv_column])
                        else:
                            context[var_name] = f"[{var_name}]"
                    
                    # Генерируем DOCX
                    doc = DocxTemplate(template_path)
                    doc.render(context)
                    
                    # Определяем имя файла
                    email = str(row.get('Email', f'row_{idx}'))
                    org = str(row.get('Организация', ''))
                    safe_email = sanitize_filename(email)
                    safe_org = sanitize_filename(org) if org else f'doc_{idx}'
                    
                    docx_filename = f"{safe_email}_{safe_org}.docx"
                    docx_path = os.path.join(output_folder, docx_filename)
                    doc.save(docx_path)
                    
                    # Конвертируем в PDF
                    try:
                        pdf_filename = f"{safe_email}_{safe_org}.pdf"
                        pdf_path = os.path.join(pdf_folder, pdf_filename)
                        convert(docx_path, pdf_path)
                    except:
                        # Если PDF не получился, продолжаем без него
                        pass
                    
                    success_count += 1
                
                except Exception as e:
                    error_list.append(f"Строка {idx + 1}: {str(e)}")
                    continue
            
            if success_count == 0:
                return jsonify({'error': f'Не удалось создать документы. Ошибки: {", ".join(error_list[:3])}'}), 400
            
            # Создаем ZIP архив
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for root, dirs, files in os.walk(output_folder):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, output_folder)
                        zip_file.write(file_path, arcname)
            
            zip_buffer.seek(0)
            
            # Очищаем временную папку
            shutil.rmtree(work_dir)
            
            # Возвращаем архив
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'documents_{timestamp}.zip'
            
            return send_file(
                zip_buffer,
                mimetype='application/zip',
                as_attachment=True,
                download_name=filename
            )
        
        except Exception as e:
            shutil.rmtree(work_dir)
            return jsonify({'error': f'Ошибка обработки: {str(e)}'}), 400
    
    except Exception as e:
        return jsonify({'error': f'Критическая ошибка: {str(e)}'}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

@app.errorhandler(413)
def too_large(e):
    return jsonify({'error': 'Файл слишком большой (максимум 50MB)'}), 413

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
